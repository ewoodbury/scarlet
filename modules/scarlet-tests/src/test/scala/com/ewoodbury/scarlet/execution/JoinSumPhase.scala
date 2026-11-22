package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.Locale

import scala.util.control.NonFatal

import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{IntPairArrays, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Phase split for cross-engine `filter-joinsum`. Not a test, and not part of `make test`.
 *
 * The input matches [[CrossEngineBench]]: keys `row % 1024`, the same LCG values, two source
 * partitions, batch 8192, shuffle width 4, two filters (`key % 3 != 0`, `value >= 0`), joined to
 * 10240 rows. The action is [[JoinValueSum.of]]. Spill stays on. The build fits the budget.
 *
 * The build arm is [[IntPairFilterEncode.tryBatches]]: one pass reads the owned arrays, applies
 * both predicates, and copies survivors. That pass is `filters`. It is the encode of the build.
 * `encode` is [[BatchCodec.copiedIntPairs]] of both inputs, the scan copy a filter-free source
 * runs. The action does not run that copy on the build, so `encode` and `filters` are not added.
 * `exchange` is [[HashExchange.partitionAll]] of those survivor batches and the probe scan.
 * `insert` is the kernel's build insert. `write` is the arm [[HashJoin.innerInt32]] selects
 * ([[JoinWrite.writeGrouped]], [[JoinWrite.writeDense]], or the per-row writer). `sum` reads the
 * value columns `write` already produced. Phases are not subtracted.
 *
 * `steady_ms` is the upper median of the last 10 rounds. `alloc_mb` is the median of five
 * process-wide thread-byte deltas after the timed rounds, in decimal megabytes. The allocation
 * probe is off during the timed rounds.
 *
 * {{{
 * JOIN_PHASE_ROWS=10000000 JOIN_PHASE_WARMUP=80 JOIN_PHASE_ROUNDS=30 \
 *   JOIN_PHASE_THREADS=4 JOIN_PHASE_ONLY=joinsum \
 *   java -Xmx10g -cp "$CP" com.ewoodbury.scarlet.execution.JoinPhaseBench
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Throw"))
object JoinSumPhase:

  private val rows = readInt("JOIN_PHASE_ROWS", 1000000)
  private val groups = 1024
  private val joinRows = 10 * groups
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val width = 4
  private val warmup = readInt("JOIN_PHASE_WARMUP", 80)
  private val rounds = readInt("JOIN_PHASE_ROUNDS", 30)
  private val threads = readInt("JOIN_PHASE_THREADS", 4)
  private val allocRounds = 5
  private val lcgSeed = 0x12345678
  private val expectedShape =
    "HashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(Scan))"
  private val known = Map(
    1000000 -> 51631715699610L,
    10000000 -> 515818008297280L,
  )
  private val knownRows = Map(
    10000000 -> 33281390L,
  )

  private final class HarnessRefusal(message: String) extends RuntimeException(message)

  private def refuse(message: String): Nothing =
    throw new HarnessRefusal(message)

  private final case class Part(build: ColumnBatch, probe: ColumnBatch, table: JoinPhaseOps.Table)

  private final case class Prepared(
      rawLeft: Plan[_],
      filtered: Plan[_],
      filteredOp: PhysicalOp,
      right: Plan[_],
      buildBatches: Vector[ColumnBatch],
      probeBatches: Vector[ColumnBatch],
      buildBuckets: Vector[ColumnBatch],
      probeBuckets: Vector[ColumnBatch],
      parts: Vector[Part],
      written: Vector[ColumnBatch],
      outputRows: Long,
      survivors: Long,
      checksum: Long,
      writer: String,
      maxChain: Int,
      probeRowsMin: Int,
      probeRowsMax: Int,
      batches: Long,
      fused: Long,
      action: () => Long,
  )

  private final case class Reading(
      phase: String,
      steadyMs: Double,
      verdict: String,
      allocMb: Double,
      checksum: Option[Long],
  )

  private final case class Steady(ms: Double, head: Double, verdict: String)

  def run(): Unit =
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(threads)
    var status = 0
    try
      configure(savedConf)
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      val prepared = prepare()
      accept(prepared)
      preamble(prepared)
      time("encode")(encodeOwned(prepared.rawLeft) ++ encodeOwned(prepared.right)) { encoded =>
        val total = rowCount(encoded)
        if (total != rows.toLong + joinRows.toLong) then
          refuse(s"encoded $total rows, expected ${rows + joinRows}")
      }
      time("filters")(filterBuild(prepared.filtered, prepared.filteredOp)) { filtered =>
        if (rowCount(filtered) != prepared.survivors) then
          refuse(s"filters kept ${rowCount(filtered)}, expected ${prepared.survivors}")
      }
      time("exchange")(exchange(prepared.buildBatches, prepared.probeBatches)) {
        case (build, probe) =>
          if (rowCount(build) != prepared.survivors) then refuse("exchange dropped a survivor")
          if (rowCount(probe) != joinRows.toLong) then refuse("exchange dropped a probe row")
      }
      time("insert")(insertAll(prepared.buildBuckets)) { tables =>
        val built = tables.foldLeft(0L)((sum, table) => sum + table.rows.toLong)
        if (built != prepared.survivors) then refuse(s"insert built $built rows")
      }
      time("write")(writeAll(prepared.parts)) { batches =>
        if (rowCount(batches) != prepared.outputRows) then refuse("write row count")
        if (JoinValueSum.ofColumns(batches) != prepared.checksum) then refuse("write checksum")
      }
      time("sum", Some(prepared.checksum))(JoinValueSum.ofColumns(prepared.written)) { summed =>
        if (summed != prepared.checksum) then refuse(s"sum $summed != ${prepared.checksum}")
      }
      time("action", Some(prepared.checksum), () => ScarletRuntime.get.shuffle.clear())(
        prepared.action(),
      ) { summed =>
        if (summed != prepared.checksum) then refuse(s"action $summed != ${prepared.checksum}")
      }
      System.err.println(
        s"join-sum writer=${prepared.writer} max_chain=${prepared.maxChain} " +
          s"probe_rows=${prepared.probeRowsMin}-${prepared.probeRowsMax}",
      )
    catch
      case refused: HarnessRefusal =>
        System.err.println(refused.getMessage)
        status = 1
      case NonFatal(thrown) =>
        thrown.printStackTrace(System.err)
        status = 1
    finally
      try scheduler.shutdown()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)
    if (status != 0) then sys.exit(status)

  private def prepare(): Prepared =
    val values = lcgValues(rows)
    val keys = Array.tabulate(rows)(row => row % groups)
    val joinKeys = Array.tabulate(joinRows)(row => row % groups)
    val joinValues = Array.tabulate(joinRows)(row => row % 100)
    val rawLeft = DistCollection.ofIntPairs(keys, values, sourcePartitions)
    val filtered = rawLeft
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val right = DistCollection.ofIntPairs(joinKeys, joinValues, sourcePartitions)
    val joined = filtered.join(right)
    val shape =
      ColumnarPlanner.lower(joined.plan).map(_.shape).getOrElse(refuse("join did not lower"))
    shape match
      case `expectedShape` => ()
      case other => refuse(s"plan was $other")
    val filteredOp =
      ColumnarPlanner.lower(filtered.plan).getOrElse(refuse("filters did not lower"))
    val buildBatches = filterBuild(filtered.plan, filteredOp)
    val probeBatches = encodeOwned(right.plan)
    val (buildBuckets, probeBuckets) = exchange(buildBatches, probeBatches)
    val tables = buildBuckets.map(JoinPhaseOps.insertKernel)
    val parts = buildBuckets.zip(probeBuckets).zip(tables).map { case ((build, probe), table) =>
      Part(build, probe, table)
    }
    val names = parts.map(part => JoinPhaseOps.writer(part.table, part.build, part.probe))
    val active = names.filter {
      case "empty" => false
      case _ => true
    }
    val writer = active.distinct match
      case Vector(name) => name
      case other =>
        refuse(
          s"writers diverged or wrote nothing: ${other.mkString(",")} from ${names.mkString(",")}",
        )
    val (_, chainMax) = bounds(parts.map(part => JoinPhaseOps.longestChain(part.table)))
    val (probeRowsMin, probeRowsMax) = bounds(parts.map(part => part.probe.length))
    val written = writeAll(parts)
    val outputRows = rowCount(written)
    val survivors = rowCount(buildBatches)
    ScarletRuntime.get.shuffle.clear()
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    val checksum =
      try JoinValueSum.of(joined)
      finally ColumnMetrics.restore(previous)
    val writtenSum = JoinValueSum.ofColumns(written)
    if (writtenSum != checksum) then refuse(s"writer sum $writtenSum != action $checksum")
    Prepared(
      rawLeft.plan,
      filtered.plan,
      filteredOp,
      right.plan,
      buildBatches,
      probeBatches,
      buildBuckets,
      probeBuckets,
      parts,
      written,
      outputRows,
      survivors,
      checksum,
      writer,
      chainMax,
      probeRowsMin,
      probeRowsMax,
      sink.batchCount,
      sink.fusedCount,
      () => JoinValueSum.of(joined),
    )

  private def accept(prepared: Prepared): Unit =
    known.get(rows).foreach { expected =>
      if (prepared.checksum != expected) then refuse(s"checksum ${prepared.checksum} != $expected")
    }
    knownRows.get(rows).foreach { expected =>
      if (prepared.outputRows != expected) then
        refuse(s"output rows ${prepared.outputRows} != $expected")
    }
    if (prepared.survivors * 10L != prepared.outputRows) then
      refuse(s"survivors ${prepared.survivors} * 10 != ${prepared.outputRows}")
    if (prepared.batches == 0L || prepared.fused != 0L) then
      refuse(s"action published batches=${prepared.batches} fused=${prepared.fused}")
    if (rowCount(prepared.probeBatches) != joinRows.toLong) then
      refuse("probe encode dropped a row")

  private def bounds(values: Vector[Int]): (Int, Int) =
    val iterator = values.iterator
    if (!iterator.hasNext) then refuse("join produced no partition")
    var low = iterator.next()
    var high = low
    while (iterator.hasNext) do
      val value = iterator.next()
      if (value < low) then low = value
      if (value > high) then high = value
    (low, high)

  private def filterBuild(plan: Plan[_], op: PhysicalOp): Vector[ColumnBatch] =
    IntPairFilterEncode
      .tryBatches(plan, op, Set(0, 1))
      .getOrElse(refuse("build filter did not fuse"))

  private def exchange(
      build: Vector[ColumnBatch],
      probe: Vector[ColumnBatch],
  ): (Vector[ColumnBatch], Vector[ColumnBatch]) =
    val built = HashExchange.partitionAll(build, keyOrdinal = 0, width, threads)
    val probed = HashExchange.partitionAll(probe, keyOrdinal = 0, width, threads)
    (built, probed)

  private def insertAll(buckets: Vector[ColumnBatch]): Vector[JoinPhaseOps.Table] =
    MorselScheduler.map(buckets, threads)((_, batch) => JoinPhaseOps.insertKernel(batch))

  private def writeAll(parts: Vector[Part]): Vector[ColumnBatch] =
    MorselScheduler.map(parts, threads) { (_, part) =>
      JoinPhaseOps.write(part.table, part.build, part.probe)
    }

  private def encodeOwned(plan: Plan[_]): Vector[ColumnBatch] =
    plan match
      case source: Plan.Source[_] =>
        source.partitions.map { partition =>
          partition.data match
            case pairs: IntPairArrays => BatchCodec.copiedIntPairs(pairs.keys, pairs.values)
            case _ => refuse("source partition was not an owned int pair array")
        }.toVector
      case other => refuse(s"encode expected a source, found ${other.getClass.getName}")

  private def time[A](
      phase: String,
      checksum: Option[Long] = None,
      before: () => Unit = () => (),
  )(body: => A)(check: A => Unit): Unit =
    System.err.println(s"sample $phase threads=$threads")
    val steady = sample(before)(body)(check)
    val allocMb = sampleAlloc(before)(body)
    val reading = Reading(phase, steady.ms, steady.verdict, allocMb, checksum)
    println(line(reading))

  private def sample[A](before: () => Unit)(body: => A)(check: A => Unit): Steady =
    var round = 0
    while (round < warmup) do
      before()
      check(body)
      round += 1
    val samples = new Array[Double](rounds)
    round = 0
    while (round < rounds) do
      val sink = new CountingSink
      val previous = ColumnMetrics.install(sink)
      try
        before()
        val started = System.nanoTime()
        val produced = body
        val elapsed = (System.nanoTime() - started) / 1_000_000.0
        samples(round) = elapsed
        check(produced)
      finally ColumnMetrics.restore(previous)
      round += 1
    steadyOf(samples)

  private def steadyOf(samples: Array[Double]): Steady =
    val n = samples.length
    val tailN = math.min(10, n)
    val headN = math.min(10, n)
    val tail = medianRange(samples, n - tailN, n)
    val head = medianRange(samples, 0, headN)
    if (n < 20) then Steady(tail, head, "short")
    else
      val mid = n - tailN / 2
      val left = medianRange(samples, n - tailN, mid)
      val right = medianRange(samples, mid, n)
      val low = math.min(left, right)
      val high = math.max(left, right)
      val verdict =
        if (high > low * 1.10) then "machine"
        else if (head > tail * 1.10) then "compilation"
        else "ok"
      Steady(tail, head, verdict)

  private def medianRange(samples: Array[Double], from: Int, until: Int): Double =
    val copy = java.util.Arrays.copyOfRange(samples, from, until)
    java.util.Arrays.sort(copy)
    copy(copy.length / 2)

  private def sampleAlloc[A](before: () => Unit)(body: => A): Double =
    startProbe() match
      case None => Double.NaN
      case Some(probe) =>
        try
          val taken = new Array[Double](allocRounds)
          var round = 0
          while (round < allocRounds) do
            val (_, bytes) = probe.account {
              before()
              body
            }
            taken(round) = bytes.toDouble / 1000000.0
            round += 1
          java.util.Arrays.sort(taken)
          taken(taken.length / 2)
        finally probe.stop()

  private def startProbe(): Option[AllocProbe] =
    ManagementFactory.getThreadMXBean match
      case bean: ThreadMXBean =>
        bean.setThreadAllocatedMemoryEnabled(true)
        if (bean.isThreadAllocatedMemoryEnabled) then Some(new AllocProbe(bean)) else None
      case _ => None

  private def rowCount(batches: Vector[ColumnBatch]): Long =
    batches.foldLeft(0L)((sum, batch) => sum + batch.length.toLong)

  private def configure(saved: ScarletConf): Unit =
    ScarletConf.set(
      saved.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = width,
        spillEnabled = true,
        threadPoolSize = threads,
      ),
    )

  private def preamble(prepared: Prepared): Unit =
    val runtime = ManagementFactory.getRuntimeMXBean
    System.err.println(
      s"join-sum rows=$rows output=${prepared.outputRows} survivors=${prepared.survivors} " +
        s"checksum=${prepared.checksum} writer=${prepared.writer} " +
        s"max_chain=${prepared.maxChain} probe_rows=${prepared.probeRowsMin}-${prepared.probeRowsMax} " +
        s"batches=${prepared.batches} fused=${prepared.fused} " +
        s"batch=$batchSize width=$width warmup=$warmup rounds=$rounds threads=$threads spill=true " +
        s"${affinity()} p50=upper-median-last-10 alloc=median-of-5 " +
        s"java=${runtime.getVmName} ${runtime.getVmVersion}",
    )

  private def line(reading: Reading): String =
    val checksum = reading.checksum.fold("")(value => s""""checksum":$value,""")
    "{" +
      """"schema":"join-sum-phase-1",""" +
      s""""phase":"${reading.phase}",""" +
      s""""threads":$threads,""" +
      s""""rows":$rows,""" +
      s""""steady_ms":${millis(reading.steadyMs)},""" +
      s""""steady":"${reading.verdict}",""" +
      s""""alloc_mb":${millis(reading.allocMb)},""" +
      checksum +
      """"one_jvm":true}"""

  private def millis(value: Double): String =
    String.format(Locale.ENGLISH, "%.3f", value)

  private def affinity(): String =
    val lines = java.nio.file.Files.readAllLines(java.nio.file.Path.of("/proc/self/status"))
    var found = "Cpus_allowed_list: unknown"
    val iterator = lines.iterator()
    while (iterator.hasNext) do
      val row = iterator.next()
      if (row.startsWith("Cpus_allowed_list")) then found = row
    found

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

  private def readInt(name: String, default: Int): Int =
    sys.env.get(name) match
      case None => default
      case Some(text) =>
        text.toIntOption match
          case Some(value) if value > 0 => value
          case _ => refuse(s"$name must be a positive integer, found $text")
