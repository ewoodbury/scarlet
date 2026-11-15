package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.concurrent.locks.LockSupport
import java.util.{Arrays, Locale}

import scala.util.control.NonFatal

import cats.effect.unsafe.implicits.global
import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Phase split for a real `filter.count` and `filter.countByKey`. Not a test, and not part of
 * `make test`. No kernel change.
 *
 * The input matches [[FilterPhaseBench]] and [[CrossEngineBench]]: 1e6 int pairs, keys
 * `row % 1024`, the same LCG values, two source partitions, batch 8192, two filters (`key % 3 !=
 * 0`, `value >= 0`), spill on. `CROSS_THREADS` selects 1 or 4. Omit it to run both. `CROSS_WARMUP`
 * and `CROSS_ROUNDS` default to 5 and 30. Allocation is a separate pass of `CROSS_ALLOC` rounds
 * (default 5), so the probe is not on the timed samples. p50 is the upper median, index `n/2`.
 *
 * `filter-count` is the guard: the action only. `filter-countbykey` splits the fused action.
 *
 *   - `action` is `count()` or `countByKey.collect()`, including encode and decode.
 *   - `encode` is [[SourceEncode.encodePairs]] for the ordinals the count plan demands.
 *   - `kernel` is [[FoldedCount.count]] on those batches: morsel hash, partial merge, exchange.
 *   - `dispatch` is [[MorselScheduler.map]] over the encoded morsels. The body returns the length.
 *
 * A checksum other than the pinned pair fails the process. Count is `333036`. `countByKey` is
 * `11147169`, the sum of `key * 31 + count`, and `10821571`, the xor fold.
 *
 * {{{
 * CROSS_THREADS=4 CROSS_WARMUP=5 CROSS_ROUNDS=30 \
 *   sbt -J-Xmx3g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.CountKeyPhaseBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Throw"))
object CountKeyPhaseBench:

  private val rows = 1000000
  private val groups = 1024
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val shuffleWidth = 4
  private val lcgSeed = 0x12345678
  private val countChecksum = 333036L
  private val xorChecksum = 10821571L
  private val crossChecksum = 11147169L

  private final class HarnessRefusal(message: String) extends RuntimeException(message)

  private final case class Reading(
      query: String,
      phase: String,
      threads: Int,
      p50Ms: Double,
      allocMb: Double,
      checksum: Long,
      encodes: Long,
  )

  private final case class ActionNotes(checksum: Long, encodes: Long, batches: Long, passes: Int)

  def main(args: Array[String]): Unit =
    val warmup = envInt("CROSS_WARMUP", 5)
    val rounds = envInt("CROSS_ROUNDS", 30)
    val allocRounds = envInt("CROSS_ALLOC", 5)
    val threads = sys.env.get("CROSS_THREADS").map(parseThreads)
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val widths = threads match
      case Some(width) => Vector(width)
      case None => Vector(1, 4)
    // Prepare hashes at four threads, so that scheduler exists even when only one width is timed.
    val schedulers = Vector(1, 4).map(width => width -> new LocalTaskScheduler(width)).toMap
    var status = 0
    try
      if (warmup < 0 || rounds <= 0 || allocRounds <= 0) then
        refuse("warmup must be >= 0 and rounds and alloc rounds must be > 0")
      val prepared = prepare(savedConf, savedRuntime, schedulers)
      preamble(warmup, rounds, allocRounds, prepared)
      val readings = widths.flatMap { width =>
        install(savedConf, savedRuntime, schedulers, width)
        Vector(
          timeQuery("filter-count", "action", width, warmup, rounds, allocRounds) {
            countAction(prepared)
          },
          timeQuery("filter-countbykey", "action", width, warmup, rounds, allocRounds) {
            countByKeyAction(prepared)
          },
          timeQuery("filter-countbykey", "encode", width, warmup, rounds, allocRounds) {
            encodePhase(prepared)
          },
          timeQuery("filter-countbykey", "kernel", width, warmup, rounds, allocRounds) {
            kernelPhase(prepared)
          },
          timeQuery("filter-countbykey", "dispatch", width, warmup, rounds, allocRounds) {
            dispatchPhase(prepared, width)
          },
        )
      }
      System.err.println(table(readings))
    catch
      case _: HarnessRefusal => status = 1
      case NonFatal(thrown) =>
        thrown.printStackTrace(System.err)
        status = 1
    finally
      try schedulers.values.foreach(_.shutdown().unsafeRunSync())
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)
    if (status != 0) then sys.exit(status)

  private final case class Prepared(
      countRun: () => Long,
      countByKeyRun: () => Long,
      partitions: Seq[Partition[_]],
      ordinals: Set[Int],
      steps: List[Selection.Step],
      batches: Vector[ColumnBatch],
      morsels: Vector[Morsel],
  )

  private def prepare(
      saved: ScarletConf,
      runtime: ScarletRuntime.RuntimeComponents,
      schedulers: Map[Int, LocalTaskScheduler],
  ): Prepared =
    install(saved, runtime, schedulers, widths = 4)
    val values = lcgValues(rows)
    val keys = Array.tabulate(rows)(row => row % groups)
    if (crossOracle(keys, values) != crossChecksum) then
      refuse(s"oracle cross ${crossOracle(keys, values)} != $crossChecksum")
    if (FilterCountChecksum.expected(keys, values, groups) != xorChecksum) then
      refuse("oracle xor drifted")
    val filtered = DistCollection
      .ofIntPairs(keys, values, sourcePartitions)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val counted = filtered.countByKey
    val plan = counted.plan
    val op = lowered(plan, "CountByKey(Exchange(FilterValue(FilterKey(Scan))))")
    val (source, child) = (plan, op) match
      case (
            Plan.CountByKeyOp(inner),
            PhysicalOp.CountByKey(PhysicalOp.Exchange(exchangeChild)),
          ) =>
        (inner, exchangeChild)
      case _ => refuse(s"countByKey lowered to ${op.shape}")
    val ordinals = ColumnUse.ordinals(plan, op)
    val steps = countSteps(source, child, ordinals)
    val partitions = scanPartitions(source, child)
    val batches = SourceEncode.encodePairs(partitions, ordinals)
    acceptRows(batches)
    val morsels = batches.flatMap(batch => Morsels.of(batch, batchSize))
    if (morsels.length <= sourcePartitions) then
      refuse(s"cut ${morsels.length} morsels from $sourcePartitions partitions")
    val kernel = FoldedCount.count(batches, steps, shuffleWidth, 4, batchSize)
    kernel match
      case Some(partials) =>
        if (crossOf(partials) != crossChecksum) then
          refuse(s"setup kernel ${crossOf(partials)} != $crossChecksum")
      case None => refuse("folded count declined on the bench input")
    Prepared(
      () => filtered.count(),
      () => {
        val collected = counted.collect()
        val xor = FilterCountChecksum.collected(collected, groups)
        val cross = collected.foldLeft(0L)((sum, pair) => sum + pair._1.toLong * 31L + pair._2)
        if (xor != xorChecksum) then refuse(s"xor $xor != $xorChecksum")
        cross
      },
      partitions,
      ordinals,
      steps,
      batches,
      morsels,
    )

  private def countAction(prepared: Prepared): ActionNotes =
    ScarletRuntime.get.shuffle.clear()
    val got = prepared.countRun()
    if (got != countChecksum) then refuse(s"filter-count $got != $countChecksum")
    ActionNotes(got, ColumnMetrics.primitiveEncodeCount, ColumnMetrics.batchCount, 0)

  private def countByKeyAction(prepared: Prepared): ActionNotes =
    ScarletRuntime.get.shuffle.clear()
    FoldedCount.resetPassesForTest()
    val got = prepared.countByKeyRun()
    if (got != crossChecksum) then refuse(s"filter-countbykey $got != $crossChecksum")
    ActionNotes(
      got,
      ColumnMetrics.primitiveEncodeCount,
      ColumnMetrics.batchCount,
      FoldedCount.passesForTest,
    )

  private def encodePhase(prepared: Prepared): ActionNotes =
    val batches = SourceEncode.encodePairs(prepared.partitions, prepared.ordinals)
    acceptRows(batches)
    ActionNotes(
      rows.toLong,
      ColumnMetrics.primitiveEncodeCount,
      ColumnMetrics.batchCount,
      0,
    )

  private def kernelPhase(prepared: Prepared): ActionNotes =
    FoldedCount.resetPassesForTest()
    val counted = FoldedCount
      .count(
        prepared.batches,
        prepared.steps,
        shuffleWidth,
        ColumnarPlanner.parallelism,
        batchSize,
      )
      .getOrElse(refuse("folded count declined"))
    val got = crossOf(counted)
    if (got != crossChecksum) then refuse(s"kernel $got != $crossChecksum")
    ActionNotes(
      got,
      ColumnMetrics.primitiveEncodeCount,
      ColumnMetrics.batchCount,
      FoldedCount.passesForTest,
    )

  private def dispatchPhase(prepared: Prepared, threads: Int): ActionNotes =
    val lengths =
      MorselScheduler.map(prepared.morsels, threads)((_, morsel) => morsel.length.toLong)
    val total = lengths.foldLeft(0L)(_ + _)
    if (total != rows.toLong) then refuse(s"dispatch length $total != $rows")
    ActionNotes(total, ColumnMetrics.primitiveEncodeCount, ColumnMetrics.batchCount, 0)

  private def timeQuery(
      query: String,
      phase: String,
      threads: Int,
      warmup: Int,
      rounds: Int,
      allocRounds: Int,
  )(body: => ActionNotes): Reading =
    System.err.println(s"sample $query $phase threads=$threads")
    val samples = new Array[Double](rounds)
    var round = 0
    while (round < warmup) do
      val _ = observe(body)
      round += 1
    round = 0
    var encodes = 0L
    var checksum = 0L
    while (round < rounds) do
      val (elapsed, notes) = observe(body)
      samples(round) = elapsed
      encodes = notes.encodes
      checksum = notes.checksum
      acceptPhase(query, phase, notes)
      round += 1
    val alloc = allocMedian(allocRounds)(body)
    val reading = Reading(query, phase, threads, median(samples), alloc, checksum, encodes)
    println(line(reading))
    reading

  private def allocMedian(rounds: Int)(body: => ActionNotes): Double =
    val bean = threadBean.getOrElse(refuse("thread allocation is unavailable"))
    bean.setThreadAllocatedMemoryEnabled(true)
    if (!bean.isThreadAllocatedMemoryEnabled) then refuse("thread allocation did not enable")
    val probe = new AllocProbe(bean)
    try
      val samples = new Array[Double](rounds)
      var round = 0
      while (round < rounds) do
        val (notes, bytes) = probe.account(body)
        acceptPhase("alloc", "alloc", notes)
        samples(round) = bytes.toDouble / 1000000.0
        round += 1
      median(samples)
    finally probe.stop()

  private def observe(body: => ActionNotes): (Double, ActionNotes) =
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    try
      val started = System.nanoTime()
      val notes = body
      val elapsed = (System.nanoTime() - started) / 1_000_000.0
      (
        elapsed,
        notes.copy(
          encodes = sink.primitiveEncodeCount,
          batches = sink.batchCount,
        ),
      )
    finally ColumnMetrics.restore(previous)

  private def acceptPhase(query: String, phase: String, notes: ActionNotes): Unit =
    if (query == "filter-countbykey" && phase == "action" && notes.passes != 1) then
      refuse(s"action fold passes ${notes.passes}, expected 1")
    if (query == "filter-countbykey" && phase == "kernel" && notes.passes != 1) then
      refuse(s"kernel fold passes ${notes.passes}, expected 1")
    if (phase == "encode" && notes.encodes != sourcePartitions.toLong) then
      refuse(s"encode notes ${notes.encodes}, expected $sourcePartitions")
    if (phase == "action" && query == "filter-count" && notes.checksum != countChecksum) then
      refuse(s"count checksum ${notes.checksum}")

  private def acceptRows(batches: Vector[ColumnBatch]): Unit =
    val total = batches.foldLeft(0L)((sum, batch) => sum + batch.length.toLong)
    if (total != rows.toLong) then refuse(s"encoded $total rows, expected $rows")
    if (batches.length != sourcePartitions) then
      refuse(s"encoded ${batches.length} batches, expected $sourcePartitions")

  private def crossOf(batches: Vector[ColumnBatch]): Long =
    batches.foldLeft(0L) { (sum, batch) =>
      BatchCodec.decodeIntPairs(batch).foldLeft(sum) { (acc, pair) =>
        acc + pair._1.toLong * 31L + pair._2.toLong
      }
    }

  private def crossOracle(keys: Array[Int], values: Array[Int]): Long =
    val seen = new Array[Int](groups)
    var index = 0
    while (index < keys.length) do
      val key = keys(index)
      if (key % 3 != 0 && values(index) >= 0) then seen(key) += 1
      index += 1
    var key = 0
    var hash = 0L
    while (key < groups) do
      if (seen(key) > 0) then hash += key.toLong * 31L + seen(key).toLong
      key += 1
    hash

  private def countSteps(
      plan: Plan[_],
      op: PhysicalOp,
      ordinals: Set[Int],
  ): List[Selection.Step] =
    val compiled = PredicatePlan.compile(plan, op).getOrElse(refuse("filters did not compile"))
    val steps = PredicatePlan
      .selectionSteps(PredicatePlan.encoded(compiled, ordinals))
      .getOrElse(refuse("filters have no selection steps"))
      .toList
    if (steps.length != 2) then refuse(s"compiled ${steps.length} steps")
    steps

  private def scanPartitions(plan: Plan[_], op: PhysicalOp): Seq[Partition[_]] =
    PredicatePlan.childOf(plan, op) match
      case (_, PhysicalOp.Scan(PhysicalOp.Kind.Pairs, partitions)) =>
        if (partitions.length != sourcePartitions) then
          refuse(s"${partitions.length} source partitions, expected $sourcePartitions")
        partitions
      case (_, other) => refuse(s"expected a pair scan, found ${other.shape}")

  private def lowered(plan: Plan[_], shape: String): PhysicalOp =
    ColumnarPlanner.lower(plan) match
      case Some(op) if op.shape == shape => op
      case Some(op) => refuse(s"lower shape ${op.shape} != $shape")
      case None => refuse(s"lower refused, expected $shape")

  private def install(
      saved: ScarletConf,
      runtime: ScarletRuntime.RuntimeComponents,
      schedulers: Map[Int, LocalTaskScheduler],
      widths: Int,
  ): Unit =
    val scheduler = schedulers.getOrElse(widths, refuse(s"no scheduler for $widths"))
    ScarletConf.set(
      saved.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = shuffleWidth,
        spillEnabled = true,
        threadPoolSize = widths,
      ),
    )
    ScarletRuntime.set(runtime.copy(scheduler = scheduler))

  private def lcgValues(count: Int): Array[Int] =
    val generated = new Array[Int](count)
    var index = 0
    var state = lcgSeed
    while (index < count) do
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      generated(index) = (mixed % 2000001) - 1000000
      index += 1
    generated

  private def median(samples: Array[Double]): Double =
    Arrays.sort(samples)
    samples(samples.length / 2)

  private def preamble(
      warmup: Int,
      rounds: Int,
      allocRounds: Int,
      prepared: Prepared,
  ): Unit =
    val runtime = ManagementFactory.getRuntimeMXBean
    System.err.println(
      s"count-key-phase rows=$rows morsels=${prepared.morsels.length} batch=$batchSize " +
        s"warmup=$warmup rounds=$rounds alloc=$allocRounds spill=true p50=upper-median " +
        s"count=$countChecksum xor=$xorChecksum cross=$crossChecksum " +
        s"java=${runtime.getVmName} ${runtime.getVmVersion}",
    )

  private def table(readings: Vector[Reading]): String =
    val header = Vector(
      "",
      "| query | phase | threads | p50 ms | alloc mb | encodes |",
      "| --- | --- | ---: | ---: | ---: | ---: |",
    )
    val body = readings.map { reading =>
      s"| ${reading.query} | ${reading.phase} | ${reading.threads} | " +
        s"${millis(reading.p50Ms)} | ${millis(reading.allocMb)} | ${reading.encodes} |"
    }
    (header ++ body).mkString("\n")

  private def line(reading: Reading): String =
    "{" +
      """"schema":"count-key-phase-1",""" +
      s""""query":"${reading.query}",""" +
      s""""phase":"${reading.phase}",""" +
      s""""threads":${reading.threads},""" +
      s""""p50_ms":${millis(reading.p50Ms)},""" +
      s""""alloc_mb":${millis(reading.allocMb)},""" +
      s""""checksum":${reading.checksum},""" +
      s""""encodes":${reading.encodes}}"""

  private def millis(value: Double): String =
    String.format(Locale.ENGLISH, "%.3f", value)

  private def envInt(name: String, default: Int): Int =
    sys.env.get(name) match
      case None => default
      case Some(text) =>
        text.toIntOption match
          case Some(value) => value
          case None => refuse(s"$name must be an integer, found $text")

  private def parseThreads(text: String): Int =
    text.toIntOption match
      case Some(1) | Some(4) => text.toInt
      case _ => refuse(s"CROSS_THREADS must be 1 or 4, found $text")

  private def refuse(message: String): Nothing =
    System.err.println(message)
    throw new HarnessRefusal(message)

  private def threadBean: Option[ThreadMXBean] =
    ManagementFactory.getThreadMXBean match
      case bean: ThreadMXBean => Some(bean)
      case _: java.lang.management.ThreadMXBean => None

  /**
   * Sums bytes on every live thread except the probe. Workers have exited when the action returns,
   * so the watch reads while the body runs. Same shape as [[QueryBench]]'s probe.
   */
  private final class AllocProbe(bean: ThreadMXBean):
    private val capacity = 1024
    private val ids = new Array[Long](capacity)
    private val baseline = new Array[Long](capacity)
    private val high = new Array[Long](capacity)
    private var count = 0
    @volatile private var running = true
    @volatile private var probeId = 0L
    private val thread = new Thread(() => watch(), "scarlet-alloc-probe")

    thread.setDaemon(true)
    thread.start()
    while (probeId == 0L) do LockSupport.parkNanos(10000L)

    def stop(): Unit =
      running = false
      LockSupport.unpark(thread)
      thread.join(1000L)
      ()

    def account(body: => ActionNotes): (ActionNotes, Long) =
      open()
      try
        val value = body
        (value, close())
      catch
        case thrown: Throwable =>
          closeQuietly()
          throw thrown

    private def watch(): Unit =
      probeId = Thread.currentThread().getId
      while (running) do
        observe(setBaseline = false)
        LockSupport.parkNanos(50000L)

    private def open(): Unit = this.synchronized {
      count = 0
      observe(setBaseline = true)
    }

    private def close(): Long = this.synchronized {
      observe(setBaseline = false)
      var index = 0
      var total = 0L
      while (index < count) do
        if (ids(index) != probeId) then
          val base = if (baseline(index) < 0L) then 0L else baseline(index)
          val delta = high(index) - base
          if (delta > 0L) then total += delta
        index += 1
      total
    }

    private def closeQuietly(): Unit =
      try
        val _ = close()
      catch case _: IllegalStateException => ()

    private def observe(setBaseline: Boolean): Unit = this.synchronized {
      val live = bean.getAllThreadIds
      var index = 0
      while (index < live.length) do
        val id = live(index)
        val bytes = bean.getThreadAllocatedBytes(id)
        if (bytes >= 0L) then note(id, bytes, setBaseline)
        index += 1
    }

    private def note(id: Long, bytes: Long, setBaseline: Boolean): Unit =
      val at = slot(id)
      if (bytes > high(at)) then high(at) = bytes
      if (setBaseline) then baseline(at) = bytes

    private def slot(id: Long): Int =
      val existing = indexOf(id)
      if (existing >= 0) then existing
      else if (count >= capacity) then
        throw new IllegalStateException(s"alloc probe filled $capacity thread slots")
      else
        val placed = count
        ids(placed) = id
        baseline(placed) = -1L
        high(placed) = 0L
        count = placed + 1
        placed

    private def indexOf(id: Long): Int =
      var index = 0
      var found = -1
      while (index < count && found < 0) do
        if (ids(index) == id) then found = index
        index += 1
      found
