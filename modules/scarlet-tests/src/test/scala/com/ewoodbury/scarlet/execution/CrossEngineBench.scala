package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory

import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{BatchCodec, ColumnBatch, ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * One-off cross-engine comparison harness. Not a test, and not part of `make test`.
 *
 * Each query builds its own input and drops it before the next query starts. `encode_ms` times one
 * encode of that input before warmup and is not subtracted from the action. The action timer still
 * includes the scan encode. A columnar action that publishes no column batch and folds nothing
 * fails the process. When [[ColumnarPlanner]] declines a plan, the arm is `row`. That is not a
 * measurement. The run fails unless the shape is listed in [[rowArms]] with a reason. An
 * allowlisted decline is not timed: `batches` and `fused` say `row`, and one execution supplies
 * the checksum. `CROSS_TIME_ROW=true` times that listed decline with the same warmup, rounds, and
 * steady window. The arm stays `row`. A published batch on a declined plan still fails the
 * process. `CROSS_TRACE_LOWERING=1` prints one lowering-refusal line on stderr before the timed
 * samples when the plan stays on the row path. The timed section leaves `traceLowering` off.
 * Stderr then prints a summary table. Its arm column is the JSON `arm`.
 *
 * Owned int arrays use [[BatchCodec.intPairs]]. Owned long arrays use
 * [[BatchCodec.copiedLongPairs]], the scan's array encode. A null long value is timed with
 * [[BatchCodec.longPairs]], the boxed scan encoder. A null int value is timed with
 * [[BatchCodec.intPairsPreservingAbsence]], and the boxed int-pair scan uses that same encoder.
 * [[BatchCodec.nullableIntPairs]] takes `Option` pairs and is not that path. `groupByKey` still
 * refuses a null value.
 *
 * Spill is enabled for this process only. The `spillEnabled` default stays false, and the spill
 * budget stays the 64 MiB default.
 *
 * `STEADY` is the median of the last 10 rounds. `compilation` means the first 10 are still above
 * that floor, so publish `steady_ms` rather than the full-window `p50_ms`. `machine` means the two
 * halves of those last 10 differ by more than 10 percent: drop the JVM. The threshold and the
 * five-JVM rule are in `docs/CROSS-ENGINE.md`.
 *
 * `CROSS_THREADS` selects the scheduler width. It defaults to 4. Allocation is a separate pass
 * after the timed rounds, the nearest-rank middle sample of [[allocRounds]] process-wide thread
 * byte deltas, in decimal megabytes. The probe is off during the timed rounds.
 *
 * {{{
 * CROSS_ROWS=10000000 CROSS_THREADS=4 CROSS_QUERY=filter-count \
 *   sbt -J-Xmx4g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.CrossEngineBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Var"))
object CrossEngineBench:

  private val groups = 1024
  private val seed = 0x12345678
  private val shuffleSeed = 58L
  private val threads: Int = readThreadCount()
  private val allocRounds = 5
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val nullStride = 4096

  /** True when the null generators plant a null value at `index`. Row 0 stays present. */
  private[execution] def nullValueAt(index: Int): Boolean =
    index % nullStride == 1

  private[execution] val queryNames = Vector(
    "filter-count",
    "filter-sum",
    "filter-groupbykey",
    "filter-reducebykey",
    "filter-join",
    "filter-joinsum",
    "filter-sort",
    "filter-countbykey",
    "filter-distinct",
    "filter-topn",
    "filter-twogroup",
    "filter-keymod",
    "filter-joinagg",
  )

  /**
   * Scored shapes that still report `row`.
   *
   * An entry is debt. `reason` says why [[ColumnarPlanner]] declines that shape, and it is not
   * optional. [[TestCrossEngineRowDebt]] fails once any harness cell of the shape lowers, so the
   * entry has to come off. A shape that reports `row` and is absent here fails the run. One name
   * covers every `(filters, nulls)` cell the harness builds. A name that is `row` for one cell and
   * columnar for another cannot stay listed: the lowered cell forces the entry off, and the
   * remaining `row` cell then fails the run.
   *
   * Empty. All thirteen names in [[queryNames]] lower for one filter and for two, with nulls and
   * without. `filter-joinagg` with nulls is refused before a plan exists, so it has no arm.
   * `filter-keymod` lowers through `MapKey` into `CountByKey`. `filter-joinagg` lowers to
   * `JoinProbeSumLeft`.
   */
  private[execution] final case class RowArm(shape: String, reason: String):
    require(reason.trim.nonEmpty, s"$shape is on the row-arm allowlist without a reason")

  private[execution] val rowArms: Vector[RowArm] = Vector.empty

  private final case class Settings(
      rows: Int,
      filters: Int,
      nulls: Boolean,
      query: Option[String],
      warmup: Int,
      rounds: Int,
      timeRow: Boolean,
      traceLowering: Boolean,
  )

  private final case class Built(action: () => Any, encodeMs: Double, plan: Plan[_])

  /** `action` is the timed body. `plan` is the plan that body runs, used to name the arm. */
  private final case class Armed(action: () => Any, plan: Plan[_])

  private final case class Sample(
      p50Ms: Double,
      checksum: Long,
      encodeMs: Double,
      batches: Long,
      fused: Long,
      allocMb: Double,
      steadyMs: Double,
      verdict: String,
      arm: String,
  )

  /** Last-10 median, first-10 median, and `ok` / `compilation` / `machine` / `short`. */
  private final case class Steady(ms: Double, head: Double, verdict: String)

  private final case class Observed(value: Any, elapsedMs: Double, batches: Long, fused: Long)

  /** Printed before throw so `finally` can restore conf, then the process exits. */
  private final class HarnessRefusal(message: String) extends RuntimeException(message)

  def main(args: Array[String]): Unit =
    val settings = readSettings()
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(threads)
    var status = 0
    try
      ScarletConf.set(
        savedConf.copy(
          threadPoolSize = threads,
          columnarExecution = true,
          columnarBatchSize = batchSize,
          defaultShufflePartitions = 4,
          spillEnabled = true,
          traceLowering = false,
        ),
      )
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      val results = run(settings)
      System.err.println(humanSummary(results))
      println(render(settings, results))
    catch case _: HarnessRefusal => status = 1
    finally
      try scheduler.shutdown()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)
    if (status != 0) then sys.exit(status)

  private def run(settings: Settings): Vector[(String, Sample)] =
    val names = settings.query match
      case Some(name) => Vector(name)
      case None => queryNames
    names.map { name =>
      name -> runQuery(name, settings)
    }

  private def runQuery(name: String, settings: Settings): Sample =
    val uptime = java.lang.management.ManagementFactory.getRuntimeMXBean.getUptime
    System.err.println(s"QUERY_BEGIN $name ${System.currentTimeMillis()} uptime=$uptime")
    val built = build(name, settings)
    val sample =
      measure(
        name,
        built,
        settings.warmup,
        settings.rounds,
        settings.timeRow,
        settings.traceLowering,
      )
    val endUptime = java.lang.management.ManagementFactory.getRuntimeMXBean.getUptime
    System.err.println(
      s"QUERY_END $name ${System.currentTimeMillis()} uptime=$endUptime " +
        s"p50=${sample.p50Ms} checksum=${sample.checksum} arm=${sample.arm}",
    )
    sample

  /** The action closes over the distributed collection, not the source arrays. */
  private def build(name: String, settings: Settings): Built =
    val rows = settings.rows
    val filters = settings.filters
    val nulls = settings.nulls
    name match
      case "filter-count" =>
        withInts(rows, filters, nulls)(data => Armed(() => data.count(), data.plan))
      case "filter-countbykey" =>
        withInts(rows, filters, nulls) { data =>
          val counted = data.countByKey
          // Checksum: sum of (key * 31L + count) over each countByKey pair.
          Armed(
            () => counted.collect().map { case (key, count) => key * 31L + count }.sum,
            counted.plan,
          )
        }
      case "filter-sum" =>
        withInts(rows, filters, nulls)(data => Armed(() => data.values.sum, data.values.plan))
      case "filter-groupbykey" =>
        withInts(rows, filters, nulls) { data =>
          val grouped = data.groupByKey
          Armed(
            () => grouped.collect().map { case (key, vs) => key * 31L + vs.size }.sum,
            grouped.plan,
          )
        }
      case "filter-reducebykey" =>
        withLongs(rows, filters, nulls) { data =>
          val reduced = data.reduceByKey[Long, Long](_ + _)
          Armed(
            () => reduced.collect().map { case (key, value) => key * 31L + value }.sum,
            reduced.plan,
          )
        }
      case "filter-join" =>
        joinQuery(rows, filters, nulls)
      case "filter-joinsum" =>
        joinSumQuery(rows, filters, nulls)
      case "filter-sort" =>
        sortQuery(rows, filters, nulls)
      case "filter-distinct" =>
        withInts(rows, filters, nulls) { data =>
          val distinct = CrossEngineShapes.distinct(data)
          Armed(() => distinct.count(), distinct.plan)
        }
      case "filter-topn" =>
        topQuery(rows, filters, nulls)
      case "filter-twogroup" =>
        withInts(rows, filters, nulls) { data =>
          val grouped = CrossEngineShapes.twoGroup(data)
          Armed(() => CrossEngineShapes.tripleChecksum(grouped.collect()), grouped.plan)
        }
      case "filter-keymod" =>
        withInts(rows, filters, nulls) { data =>
          val counted = CrossEngineShapes.keyMod(data)
          Armed(() => CrossEngineShapes.pairChecksum(counted.collect()), counted.plan)
        }
      case "filter-joinagg" =>
        joinAggQuery(rows, filters, nulls)
      case other =>
        refuse(s"unknown query $other")

  private def withInts(
      rows: Int,
      filters: Int,
      nulls: Boolean,
  )(use: DistCollection[(Int, Int)] => Armed): Built =
    val (data, encodeMs) = filteredInts(rows, filters, nulls)
    val armed = use(data)
    Built(armed.action, encodeMs, armed.plan)

  private def withLongs(
      rows: Int,
      filters: Int,
      nulls: Boolean,
  )(use: DistCollection[(Long, Long)] => Armed): Built =
    val (data, encodeMs) = filteredLongs(rows, filters, nulls)
    val armed = use(data)
    Built(armed.action, encodeMs, armed.plan)

  private def filteredInts(
      rows: Int,
      filters: Int,
      nulls: Boolean,
  ): (DistCollection[(Int, Int)], Double) =
    if (nulls) then
      val pairs = nullableInts(rows, rowKeys(rows))
      val encodeMs = timed(BatchCodec.intPairsPreservingAbsence(pairs))
      (filterInts(DistCollection(pairs, sourcePartitions), filters), encodeMs)
    else
      val keys = rowKeys(rows)
      val values = lcgValues(rows)
      val encodeMs = timed(BatchCodec.intPairs(keys, values))
      val data = DistCollection.ofIntPairs(keys, values, sourcePartitions)
      (filterInts(data, filters), encodeMs)

  private def filteredLongs(
      rows: Int,
      filters: Int,
      nulls: Boolean,
  ): (DistCollection[(Long, Long)], Double) =
    if (nulls) then
      val pairs = nullableLongs(rows)
      val encodeMs = timed(BatchCodec.longPairs(pairs))
      (filterLongs(DistCollection(pairs, sourcePartitions), filters), encodeMs)
    else
      val keys = longKeys(rows)
      val values = lcgLongs(rows)
      val encodeMs = timed(BatchCodec.copiedLongPairs(keys, values))
      val data = DistCollection.ofLongPairs(keys, values, sourcePartitions)
      (filterLongs(data, filters), encodeMs)

  private def joinQuery(rows: Int, filters: Int, nulls: Boolean): Built =
    joinBuilt(rows, filters, nulls)(joined => () => joined.count())

  /**
   * Same inputs as [[joinQuery]]. The action sums `left * 31 + right` from the hash probe and does
   * not write the match columns. The count action does not write them either.
   */
  private def joinSumQuery(rows: Int, filters: Int, nulls: Boolean): Built =
    joinBuilt(rows, filters, nulls)(joined => () => JoinValueSum.of(joined))

  private def joinBuilt(
      rows: Int,
      filters: Int,
      nulls: Boolean,
  )(use: DistCollection[(Int, (Int, Int))] => () => Any): Built =
    val width = 10 * groups
    val joinKeys = Array.tabulate(width)(row => row % groups)
    val joinValues = Array.tabulate(width)(row => row % 100)
    val side = DistCollection.ofIntPairs(joinKeys, joinValues, sourcePartitions)
    val (filtered, encodeMs) =
      if (nulls) then
        val pairs = nullableInts(rows, rowKeys(rows))
        val elapsed = timedBoth(
          BatchCodec.intPairsPreservingAbsence(pairs),
          BatchCodec.intPairs(joinKeys, joinValues),
        )
        val data = DistCollection(pairs, sourcePartitions)
        (filterInts(data, filters), elapsed)
      else
        val keys = rowKeys(rows)
        val values = lcgValues(rows)
        val elapsed = timedBoth(
          BatchCodec.intPairs(keys, values),
          BatchCodec.intPairs(joinKeys, joinValues),
        )
        val data = DistCollection.ofIntPairs(keys, values, sourcePartitions)
        (filterInts(data, filters), elapsed)
    val joined = filtered.join(side)
    Built(use(joined), encodeMs, joined.plan)

  /**
   * Two int tables, the same generators as [[joinQuery]]. Both sides are filtered. The action
   * groups by the join key and sums the left value once per match. Null values are refused: the
   * sum reads that value as a `Long`, and a null would unbox on the row path.
   */
  private def joinAggQuery(rows: Int, filters: Int, nulls: Boolean): Built =
    if (nulls) then refuse("filter-joinagg does not time null values")
    val width = 10 * groups
    val joinKeys = Array.tabulate(width)(row => row % groups)
    val joinValues = Array.tabulate(width)(row => row % 100)
    val keys = rowKeys(rows)
    val values = lcgValues(rows)
    val encodeMs = timedBoth(
      BatchCodec.intPairs(keys, values),
      BatchCodec.intPairs(joinKeys, joinValues),
    )
    val left = DistCollection.ofIntPairs(keys, values, sourcePartitions)
    val right = DistCollection.ofIntPairs(joinKeys, joinValues, sourcePartitions)
    val aggregated = CrossEngineShapes.joinAgg(left, right, filters)
    Built(
      () => CrossEngineShapes.sumChecksum(aggregated.collect()),
      encodeMs,
      aggregated.plan,
    )

  /** Pairs are `(sk, v)`. `sk` is the Fisher-Yates permutation. `v` is the shared generator. */
  private def sortSource(rows: Int, nulls: Boolean): (DistCollection[(Int, Int)], Double) =
    val order = permutation(rows)
    if (nulls) then
      val pairs = nullableInts(rows, order)
      val elapsed = timed(BatchCodec.intPairsPreservingAbsence(pairs))
      (DistCollection(pairs, sourcePartitions), elapsed)
    else
      val values = lcgValues(rows)
      val elapsed = timed(BatchCodec.intPairs(order, values))
      (DistCollection.ofIntPairs(order, values, sourcePartitions), elapsed)

  private def sortQuery(rows: Int, filters: Int, nulls: Boolean): Built =
    val (data, encodeMs) = sortSource(rows, nulls)
    val sorted = filterInts(data, filters).sortByIntKey
    // Same sum as `collect().map(key * 31 + value).sum`. Present columns are read in place.
    Built(
      () => SortChecksum.of(sorted),
      encodeMs,
      sorted.plan,
    )

  /**
   * Same pairs as [[sortQuery]]. The filter is `sk % 3 != 0` and `v >= 0`. Order is value
   * descending, then `sk` ascending, then `take(100)`.
   */
  private def topQuery(rows: Int, filters: Int, nulls: Boolean): Built =
    val (data, encodeMs) = sortSource(rows, nulls)
    val sorted = CrossEngineShapes.sorted(filterInts(data, filters))
    Built(
      () => CrossEngineShapes.pairChecksum(sorted.take(CrossEngineShapes.topLimit)),
      encodeMs,
      sorted.plan,
    )

  private def filterInts(
      data: DistCollection[(Int, Int)],
      filters: Int,
  ): DistCollection[(Int, Int)] =
    val keyed = data.filterKeys((key: Int) => key % 3 != 0)
    if (filters == 1) then keyed
    else keyed.filterValues((value: Int) => value >= 0)

  private def filterLongs(
      data: DistCollection[(Long, Long)],
      filters: Int,
  ): DistCollection[(Long, Long)] =
    val keyed = data.filterKeys((key: Long) => key % 3 != 0)
    if (filters == 1) then keyed
    else keyed.filterValues((value: Long) => value >= 0)

  private def measure(
      name: String,
      built: Built,
      warmup: Int,
      rounds: Int,
      timeRow: Boolean,
      traceLowering: Boolean,
  ): Sample =
    if (traceLowering) then traceRefusal(built.plan)
    val arm = armOf(built.plan)
    // Outside the timed loop. A columnar arm does not read the allowlist.
    rowArmRefusal(name, arm).foreach(refuse)
    arm match
      case None if timeRow => timedSample(name, built, "row", warmup, rounds, columnar = false)
      case None => rowSample(name, built)
      case Some(shape) => timedSample(name, built, shape, warmup, rounds, columnar = true)

  /**
   * `Some` when a scored shape reports `row` and [[rowArms]] does not list it. A columnar arm
   * returns `None` without reading the list.
   */
  private[execution] def rowArmRefusal(name: String, arm: Option[String]): Option[String] =
    arm match
      case Some(_) => None
      case None => unlistedRow(name, rowArms.map(_.shape))

  /** `Some` when `name` reports `row` and `listed` does not contain it. */
  private[execution] def unlistedRow(name: String, listed: Seq[String]): Option[String] =
    if (listed.contains(name)) then None
    else
      Some(
        s"$name reports arm=row and is not on the row-arm allowlist. " +
          "A row fallback is not a scored measurement.",
      )

  /**
   * Plan [[build]] would time for `name`, without running it. `filter-joinagg` with nulls refuses
   * before a plan exists.
   */
  private[execution] def scoredPlan(
      name: String,
      rows: Int,
      filters: Int,
      nulls: Boolean,
  ): Plan[_] =
    build(name, Settings(rows, filters, nulls, None, 0, 0, false, false)).plan

  /** [[armOf]] of [[scoredPlan]]. `None` is `row`. */
  private[execution] def scoredArm(
      name: String,
      rows: Int,
      filters: Int,
      nulls: Boolean,
  ): Option[String] =
    armOf(scoredPlan(name, rows, filters, nulls))

  /**
   * One refusal line, outside the timer. [[ColumnarPlanner.lower]] prints it when the flag is on
   * and the plan stays empty. A plan that lowers prints nothing. The flag is off again before
   * warmup.
   */
  private def traceRefusal(plan: Plan[_]): Unit =
    val saved = ScarletConf.get
    ScarletConf.set(saved.copy(traceLowering = true))
    try
      val _ = ColumnarPlanner.lower(plan)
    finally ScarletConf.set(saved)

  /**
   * Physical shape when [[ColumnarPlanner]] accepts `plan`. `None` is the row path, including a
   * lowered tree whose layout is invalid. This does not run the plan.
   */
  private[execution] def armOf(plan: Plan[_]): Option[String] =
    ColumnarPlanner.partitioning(plan) match
      case None => None
      case Some(_) => ColumnarPlanner.lower(plan).map(_.shape)

  /**
   * One execution, for the checksum. The timed loop stays off: a row-path p50 is not a columnar
   * speed. A batch or a fused fold here means the decline was wrong.
   */
  private def rowSample(name: String, built: Built): Sample =
    val observed = observe(built.action)
    if (observed.batches != 0L || observed.fused != 0L) then
      refuse(s"$name declined but published batches=${observed.batches} fused=${observed.fused}")
    System.err.println(s"ROW $name arm=row checksum=${checksumOf(observed.value)}")
    Sample(
      Double.NaN,
      checksumOf(observed.value),
      built.encodeMs,
      0L,
      0L,
      Double.NaN,
      Double.NaN,
      "row",
      "row",
    )

  private def timedSample(
      name: String,
      built: Built,
      shape: String,
      warmup: Int,
      rounds: Int,
      columnar: Boolean,
  ): Sample =
    var round = 0
    while (round < warmup) do
      ScarletRuntime.get.shuffle.clear()
      val _ = built.action()
      round += 1
    val samples = new Array[Double](rounds)
    var checksum = 0L
    var batches = 0L
    var fused = 0L
    round = 0
    while (round < rounds) do
      val observed = observe(built.action)
      if (columnar) then
        if (observed.batches == 0L && observed.fused == 0L) then
          refuse(s"$name published no column batch and folded nothing")
      else if (observed.batches != 0L || observed.fused != 0L) then
        refuse(s"$name declined but published batches=${observed.batches} fused=${observed.fused}")
      samples(round) = observed.elapsedMs
      checksum = checksumOf(observed.value)
      batches = observed.batches
      fused = observed.fused
      round += 1
    val steady = steadyOf(samples)
    if (
        name == "filter-join" || name == "filter-joinsum" || name == "filter-count" ||
        name == "filter-joinagg"
      )
    then
      val shown = samples.mkString(",")
      System.err.println(s"SAMPLES $name $shown")
    System.err.println(
      s"STEADY $name ${steady.verdict} steady=${steady.ms} head=${steady.head} arm=$shape",
    )
    java.util.Arrays.sort(samples)
    Sample(
      samples(samples.length / 2),
      checksum,
      built.encodeMs,
      batches,
      fused,
      sampleAlloc(built.action),
      steady.ms,
      steady.verdict,
      shape,
    )

  /**
   * Last 10 rounds are the steady floor. The first 10 are the compile slope when they are still
   * high. The two halves of the last 10 catch a stall that a single median would hide. Fewer than
   * 20 rounds leaves no separate tail, so the line is `short`.
   */
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

  /** Upper median of `[from, until)`, the same slot as the full-window p50. */
  private def medianRange(samples: Array[Double], from: Int, until: Int): Double =
    val copy = java.util.Arrays.copyOfRange(samples, from, until)
    java.util.Arrays.sort(copy)
    copy(copy.length / 2)

  private def observe(action: () => Any): Observed =
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    try
      ScarletRuntime.get.shuffle.clear()
      val started = System.nanoTime()
      val value = action()
      val elapsedMs = (System.nanoTime() - started) / 1_000_000.0
      Observed(value, elapsedMs, sink.batchCount, sink.fusedCount)
    finally ColumnMetrics.restore(previous)

  private def checksumOf(value: Any): Long = value match
    case number: Long => number
    case number: Int => number.toLong
    case other => throw new IllegalStateException(s"unexpected checksum $other")

  private def timed(encode: => ColumnBatch): Double =
    val started = System.nanoTime()
    val batch = encode
    val elapsed = (System.nanoTime() - started) / 1_000_000.0
    val _ = batch.length
    elapsed

  private def timedBoth(left: => ColumnBatch, right: => ColumnBatch): Double =
    val started = System.nanoTime()
    val first = left
    val second = right
    val elapsed = (System.nanoTime() - started) / 1_000_000.0
    val _ = first.length + second.length
    elapsed

  private def rowKeys(rows: Int): Array[Int] =
    Array.tabulate(rows)(row => row % groups)

  private def longKeys(rows: Int): Array[Long] =
    Array.tabulate(rows)(row => (row % groups).toLong)

  private def lcgValues(count: Int): Array[Int] =
    val values = new Array[Int](count)
    var index = 0
    var state = seed
    while (index < count) do
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      values(index) = (mixed % 2000001) - 1000000
      index += 1
    values

  private def lcgLongs(count: Int): Array[Long] =
    val values = lcgValues(count)
    Array.tabulate(count)(index => values(index).toLong)

  private def permutation(count: Int): Array[Int] =
    val order = Array.tabulate(count)(identity)
    var state = shuffleSeed
    var index = count - 1
    while (index > 0) do
      state = (state * 1664525 + 1013904223) & 0xffffffffL
      val other = (state % (index + 1)).toInt
      val held = order(index)
      order(index) = order(other)
      order(other) = held
      index -= 1
    order

  /**
   * Value is null when `index` is 1 mod 4096. Row 0 stays a present pair so the source classifies.
   * A dense key is `index % 1024`, so that null row's key is 1 and the key filter keeps it. Sort
   * keys are the permutation, so a sort null stays only when the permuted key is not divisible by
   *   3. The key itself stays present.
   */
  private def nullableInts(rows: Int, keys: Array[Int]): IndexedSeq[(Int, Int)] =
    val values = lcgValues(rows)
    IndexedSeq.tabulate(rows) { index =>
      if (nullValueAt(index)) then intPair(boxedInt(keys(index)), absentInt)
      else keys(index) -> values(index)
    }

  private def nullableLongs(rows: Int): IndexedSeq[(Long, Long)] =
    val keys = longKeys(rows)
    val values = lcgLongs(rows)
    IndexedSeq.tabulate(rows) { index =>
      if (nullValueAt(index)) then longPair(boxedLong(keys(index)), absentLong)
      else keys(index) -> values(index)
    }

  /** Named erasure boundary. `Tuple2` keeps a null `Integer`; `(Int, Int)` would unbox it. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def intPair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  /** Named erasure boundary. `Tuple2` keeps a null `Long`; `(Long, Long)` would unbox it. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def longPair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentInt: java.lang.Integer = null

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentLong: java.lang.Long = null

  private def boxedInt(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)

  private def boxedLong(value: Long): java.lang.Long = java.lang.Long.valueOf(value)

  /**
   * One row per shape. The arm column is [[Sample.arm]], the same value as the JSON `arm`,
   * including `row`.
   */
  private def humanSummary(results: Vector[(String, Sample)]): String =
    summaryTable(
      results.map { case (name, sample) =>
        (name, sample.arm, jsonDouble(sample.p50Ms), sample.checksum, sample.verdict)
      },
    )

  /** Header plus one body row. `arm` is the second column. */
  private[execution] def summaryTable(
      rows: Seq[(String, String, String, Long, String)],
  ): String =
    val header = Vector(
      "| shape | arm | p50_ms | checksum | steady |",
      "| --- | --- | ---: | ---: | --- |",
    )
    val body = rows.map { case (name, arm, p50, checksum, verdict) =>
      s"| $name | $arm | $p50 | $checksum | $verdict |"
    }
    (header ++ body).mkString("\n")

  private def render(settings: Settings, results: Vector[(String, Sample)]): String =
    val body = results.map { case (name, sample) => queryJson(name, sample) }.mkString(",\n")
    s"""{
  "engine": "scarlet",
  "rows": ${settings.rows},
  "threads": $threads,
  "warmup": ${settings.warmup},
  "rounds": ${settings.rounds},
  "filters": ${settings.filters},
  "nulls": ${settings.nulls},
  "spill": true,
  "results": {
$body
  }
}"""

  private def queryJson(name: String, sample: Sample): String =
    val alloc = jsonDouble(sample.allocMb)
    val p50 = jsonDouble(sample.p50Ms)
    val steadyMs = jsonDouble(sample.steadyMs)
    val (batches, fused) = sample.arm match
      case "row" => ("\"row\"", "\"row\"")
      case _ => (sample.batches.toString, sample.fused.toString)
    val head = s"""    "$name": {"p50_ms": $p50, "checksum": ${sample.checksum},"""
    val steady = s""" "steady_ms": $steadyMs, "steady": "${sample.verdict}","""
    val tail =
      s""" "alloc_mb": $alloc, "encode_ms": ${sample.encodeMs}, "batches": $batches, "fused": $fused, "arm": "${sample.arm}"}"""
    head + steady + tail

  private def jsonDouble(value: Double): String =
    if (value.isNaN) then "null" else value.toString

  /** Middle sample of [[allocRounds]] process-wide byte deltas, in decimal megabytes. */
  private def sampleAlloc(action: () => Any): Double =
    startProbe() match
      case None => Double.NaN
      case Some(probe) =>
        try
          val samples = new Array[Double](allocRounds)
          var round = 0
          while (round < allocRounds) do
            val (_, bytes) = probe.account {
              ScarletRuntime.get.shuffle.clear()
              val _ = action()
            }
            samples(round) = bytes.toDouble / 1_000_000.0
            round += 1
          java.util.Arrays.sort(samples)
          samples(samples.length / 2)
        finally probe.stop()

  private def startProbe(): Option[AllocProbe] =
    threadBean.filter(_.isThreadAllocatedMemorySupported).flatMap { bean =>
      bean.setThreadAllocatedMemoryEnabled(true)
      if (bean.isThreadAllocatedMemoryEnabled) then Some(new AllocProbe(bean)) else None
    }

  private def threadBean: Option[ThreadMXBean] =
    ManagementFactory.getThreadMXBean match
      case bean: ThreadMXBean => Some(bean)
      case _ => None

  private def readThreadCount(): Int =
    readInt("CROSS_THREADS", 4) match
      case value if value > 0 => value
      case value => envError(s"CROSS_THREADS must be a positive integer, found $value")

  private def readSettings(): Settings =
    Settings(
      rows = readInt("CROSS_ROWS", 1_000_000),
      filters = readChoice("CROSS_FILTERS", 2, Set(1, 2)),
      nulls = readFlag("CROSS_NULLS", false),
      query = readQuery(),
      // Warmup 5 still leaves filter-join falling after round 5. 80 is the depth whose
      // last 10 rounds sit on the floor. See docs/CROSS-ENGINE.md.
      warmup = readInt("CROSS_WARMUP", 80),
      rounds = readInt("CROSS_ROUNDS", 30),
      timeRow = readFlag("CROSS_TIME_ROW", false),
      traceLowering = readOne("CROSS_TRACE_LOWERING"),
    )

  private def readInt(name: String, default: Int): Int =
    sys.env.get(name) match
      case None => default
      case Some(text) =>
        text.toIntOption match
          case Some(value) => value
          case None => envError(s"$name must be an integer, found $text")

  private def readChoice(name: String, default: Int, allowed: Set[Int]): Int =
    val value = readInt(name, default)
    if (allowed.contains(value)) then value
    else envError(s"$name must be ${allowed.mkString(" or ")}, found $value")

  /** `1` turns the flag on. Unset leaves it off. Any other value stops the process. */
  private def readOne(name: String): Boolean =
    sys.env.get(name) match
      case None => false
      case Some("1") => true
      case Some(other) => envError(s"$name must be 1, found $other")

  private def readFlag(name: String, default: Boolean): Boolean =
    sys.env.get(name) match
      case None => default
      case Some("true") => true
      case Some("false") => false
      case Some(other) => envError(s"$name must be true or false, found $other")

  private def readQuery(): Option[String] =
    sys.env.get("CROSS_QUERY") match
      case None => None
      case Some(name) if queryNames.contains(name) => Some(name)
      case Some(name) =>
        envError(s"CROSS_QUERY must name one of ${queryNames.mkString(", ")}, found $name")

  private def envError(message: String): Nothing =
    System.err.println(message)
    sys.exit(1)

  private def refuse(message: String): Nothing =
    System.err.println(message)
    throw new HarnessRefusal(message)
