package com.ewoodbury.scarlet.execution
import java.lang.management.ManagementFactory
import java.util.{Arrays, Locale}

import scala.util.control.NonFatal

import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler
import com.ewoodbury.scarlet.support.Check

/**
 * Phase split for a real `filter.count` and `filter.countByKey`. Not a test, and not part of
 * `make test`.
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
 *   - `kernel` is [[FoldedCount.count]]. A morsel whose present keys span fewer than 4096 values
 *     increments a dense slot; every other morsel still hash-inserts. Then the partials merge.
 *   - `dispatch` is [[MorselScheduler.map]] over the encoded morsels. The body returns the length.
 *   - `scan` reads the key and value arrays. `predicate` applies both filters. `predicate-call`
 *     calls the underlying `Int => Boolean` instead of the trait adapter.
 *   - `insert` is the open-addressed insert the dense path replaces, on the survivors and the
 *     production table size. `insert-fit` sizes that table for 1024 groups. Neither is the live
 *     kernel. `mix` is only [[ColumnHash.mix]].
 *   - `merge` exchanges partials built outside the timer. `output` decodes a finished result.
 *   - `direct` repeats the dense count and its merge outside [[FoldedCount]].
 *
 * Passes are not subtracted from each other. A parallel pass includes its morsel dispatch.
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
          timeQuery("filter-countbykey", "scan", width, warmup, rounds, allocRounds) {
            scanPhase(prepared, width)
          },
          timeQuery("filter-countbykey", "predicate", width, warmup, rounds, allocRounds) {
            predicatePhase(prepared, width, directCall = false)
          },
          timeQuery("filter-countbykey", "predicate-call", width, warmup, rounds, allocRounds) {
            predicatePhase(prepared, width, directCall = true)
          },
          timeQuery("filter-countbykey", "insert", width, warmup, rounds, allocRounds) {
            insertPhase(prepared, width, fit = false)
          },
          timeQuery("filter-countbykey", "insert-fit", width, warmup, rounds, allocRounds) {
            insertPhase(prepared, width, fit = true)
          },
          timeQuery("filter-countbykey", "mix", width, warmup, rounds, allocRounds) {
            mixPhase(prepared, width)
          },
          timeQuery("filter-countbykey", "direct", width, warmup, rounds, allocRounds) {
            directPhase(prepared, width)
          },
          timeQuery("filter-countbykey", "merge", width, warmup, rounds, allocRounds) {
            mergePhase(prepared, width)
          },
          timeQuery("filter-countbykey", "output", width, warmup, rounds, allocRounds) {
            outputPhase(prepared)
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
      try schedulers.values.foreach(_.shutdown())
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
      work: CountKeyPhases.Work,
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
    val kernelBatches = kernel match
      case Some(partials) =>
        if (crossOf(partials) != crossChecksum) then
          refuse(s"setup kernel ${crossOf(partials)} != $crossChecksum")
        partials
      case None => refuse("folded count declined on the bench input")
    val work = CountKeyPhases.prepare(batches, morsels, steps, shuffleWidth, batchSize)
    if (CountKeyPhases.cross(work.finished) != crossChecksum) then
      refuse(s"hash partials ${CountKeyPhases.cross(work.finished)} != $crossChecksum")
    if (CountKeyPhases.fingerprint(work.finished) != CountKeyPhases.fingerprint(kernelBatches))
    then refuse("hash partial order drifted from FoldedCount")
    if (CountKeyPhases.cross(work.directSample) != crossChecksum) then
      refuse(s"direct ${CountKeyPhases.cross(work.directSample)} != $crossChecksum")
    if (CountKeyPhases.fingerprint(work.directSample) != CountKeyPhases.fingerprint(kernelBatches))
    then refuse("direct order drifted from FoldedCount")
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
      work,
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

  private def scanPhase(prepared: Prepared, threads: Int): ActionNotes =
    val got = CountKeyPhases.scan(prepared.work, threads)
    if (got != prepared.work.scanChecksum) then
      refuse(s"scan $got != ${prepared.work.scanChecksum}")
    ActionNotes(got, 0, 0, 0)

  private def predicatePhase(prepared: Prepared, threads: Int, directCall: Boolean): ActionNotes =
    val got = CountKeyPhases.predicate(prepared.work, threads, directCall)
    if (got != countChecksum) then refuse(s"predicate $got != $countChecksum")
    ActionNotes(got, 0, 0, 0)

  private def insertPhase(prepared: Prepared, threads: Int, fit: Boolean): ActionNotes =
    val got = CountKeyPhases.insert(prepared.work, threads, fit)
    if (got != countChecksum) then refuse(s"insert $got != $countChecksum")
    ActionNotes(got, 0, 0, 0)

  private def mixPhase(prepared: Prepared, threads: Int): ActionNotes =
    val got = CountKeyPhases.mix(prepared.work, threads)
    if (got != prepared.work.mixChecksum) then refuse(s"mix $got != ${prepared.work.mixChecksum}")
    ActionNotes(got, 0, 0, 0)

  private def directPhase(prepared: Prepared, threads: Int): ActionNotes =
    val counted = CountKeyPhases.direct(
      prepared.morsels,
      prepared.steps,
      shuffleWidth,
      batchSize,
      threads,
    )
    val got = CountKeyPhases.cross(counted)
    if (got != crossChecksum) then refuse(s"direct $got != $crossChecksum")
    if (
        CountKeyPhases
          .fingerprint(counted) != CountKeyPhases.fingerprint(prepared.work.directSample)
      )
    then refuse("direct order drifted across a timed round")
    ActionNotes(got, 0, 0, 0)

  private def mergePhase(prepared: Prepared, threads: Int): ActionNotes =
    val got = CountKeyPhases.merge(prepared.work, threads)
    if (got != crossChecksum) then refuse(s"merge $got != $crossChecksum")
    ActionNotes(got, 0, 0, 0)

  private def outputPhase(prepared: Prepared): ActionNotes =
    val got = CountKeyPhases.output(prepared.work)
    if (got != crossChecksum) then refuse(s"output $got != $crossChecksum")
    ActionNotes(got, 0, 0, 0)

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
    if (Check.same(query, "filter-countbykey") && Check.same(phase, "action") && notes.passes != 1)
    then refuse(s"action fold passes ${notes.passes}, expected 1")
    if (Check.same(query, "filter-countbykey") && Check.same(phase, "kernel") && notes.passes != 1)
    then refuse(s"kernel fold passes ${notes.passes}, expected 1")
    if (Check.same(phase, "encode") && notes.encodes != sourcePartitions.toLong) then
      refuse(s"encode notes ${notes.encodes}, expected $sourcePartitions")
    if (
        Check.same(phase, "action") && Check.same(
          query,
          "filter-count",
        ) && notes.checksum != countChecksum
      )
    then refuse(s"count checksum ${notes.checksum}")

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
      case Some(op) if Check.same(op.shape, shape) => op
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
        s"extraProbes=${prepared.work.extraProbes} inserted=${prepared.work.inserted} " +
        s"left=${prepared.work.predicateClass} right=${prepared.work.directClass} " +
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
