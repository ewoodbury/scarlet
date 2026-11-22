package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, HashCount, JoinWriteRuns}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Same-session times for `countByKey` of a sort-merge, a triple filter, then `(key, left.toLong)`.
 *
 * The inputs match [[GenericJoinCountBench]]: left keys `row % 1024`, the shared LCG values, right
 * keys `row % 1024` over `10 * 1024` rows with values `row % 100`. Two filters on the inputs. The
 * join is `sortMergeJoin`. One whole-row filter, `left > 10`, sits between the join and the map.
 * [[GenericJoinCountBench]] and [[GenericSortMergeCountBench]] stay on the unfiltered join.
 *
 * `CROSS_MODE=row` times only the row path. `CROSS_MODE=both` times the row path, then the
 * columnar path, in one JVM. Spill is on for this process. Warmup defaults to 80 and rounds to 30.
 *
 * {{{
 * CROSS_ROWS=1000000 CROSS_FILTERS=2 CROSS_THREADS=4 CROSS_WARMUP=80 CROSS_ROUNDS=30 \
 *   CROSS_MODE=both sbt -J-Xmx4g \
 *   "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.FilteredSortMergeCountBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Var"))
object FilteredSortMergeCountBench:

  private val groups = 1024
  private val seed = 0x12345678
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val expectedAt1e6 = 12486132L

  private final case class Arm(
      name: String,
      p50Ms: Double,
      steadyMs: Double,
      steady: String,
      checksum: Long,
      batches: Long,
      fused: Long,
      sortMergeTasks: Int,
      byKeyCalls: Int,
      joinWriteRuns: Long,
  )

  def main(args: Array[String]): Unit =
    val rows = readPositive("CROSS_ROWS", 1000000)
    val filters = readChoice("CROSS_FILTERS", 2, Set(1, 2))
    val threads = readPositive("CROSS_THREADS", 4)
    val warmup = readPositive("CROSS_WARMUP", 80)
    val rounds = readPositive("CROSS_ROUNDS", 30)
    val mode = readMode()
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(threads)
    try
      ScarletConf.set(
        savedConf.copy(
          threadPoolSize = threads,
          columnarExecution = true,
          columnarBatchSize = batchSize,
          defaultShufflePartitions = 4,
          spillEnabled = true,
        ),
      )
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      val counted = query(rows, filters)
      val shape = ColumnarPlanner.lower(counted.plan).map(_.shape).getOrElse("row")
      println(s"arm $shape")
      println(s"sort_merge_in_arm ${shape.contains("SortMergeJoin")}")
      println(s"hash_join_in_arm ${shape.contains("HashJoin")}")
      println(s"project_filter ${shape.contains("Project(Filter")}")
      val row = time("row", counted.plan, warmup, rounds, columnar = false)
      if (row.batches != 0L || row.fused != 0L) then
        refuse(s"row published batches=${row.batches} fused=${row.fused}")
      if (row.sortMergeTasks == 0) then refuse("row path did not run SortMergeJoinTask")
      if (rows == 1000000 && filters == 2 && row.checksum != expectedAt1e6) then
        refuse(s"row checksum ${row.checksum} != $expectedAt1e6")
      println(render(row))
      mode match
        case "both" =>
          val columnar = time("columnar", counted.plan, warmup, rounds, columnar = true)
          if (row.checksum != columnar.checksum) then
            refuse(s"checksums differ row=${row.checksum} columnar=${columnar.checksum}")
          if (columnar.batches == 0L && columnar.fused == 0L) then
            refuse("columnar published no column batch and folded nothing")
          if (columnar.sortMergeTasks != 0) then
            refuse(s"columnar ran SortMergeJoinTask ${columnar.sortMergeTasks}")
          val ratio = row.p50Ms / columnar.p50Ms
          println(render(columnar))
          println(f"ratio_row_over_columnar $ratio%.3f")
          val probe =
            shape.contains("CountByKey") && shape.contains("HashJoin") &&
              shape.contains("Project(Filter") && !shape.contains("SortMergeJoin") &&
              !shape.contains("BroadcastJoin")
          if (!probe) then refuse(s"columnar arm is not a filtered probed count: $shape")
          if (columnar.byKeyCalls != 0) then
            refuse(s"HashCount.byKey ran ${columnar.byKeyCalls} times")
          if (columnar.joinWriteRuns != 0L) then
            refuse(s"JoinWrite ran ${columnar.joinWriteRuns} times")
        case _ => ()
    finally
      try scheduler.shutdown()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  /**
   * Filtered int sort-merge, one whole-row filter on the left value, then `(key, left.toLong)` and
   * `countByKey`. The map is not `sumJoinLeft`. The predicate body is not specialized here.
   */
  private def query(rows: Int, filters: Int): DistCollection[(Int, Int)] =
    val width = 10 * groups
    val joinKeys = Array.tabulate(width)(row => row % groups)
    val joinValues = Array.tabulate(width)(row => row % 100)
    val keys = Array.tabulate(rows)(row => row % groups)
    val values = lcgValues(rows)
    val left = DistCollection.ofIntPairs(keys, values, sourcePartitions)
    val right = DistCollection.ofIntPairs(joinKeys, joinValues, sourcePartitions)
    val filteredLeft = applyFilters(left, filters, (value: Int) => value >= 0)
    val filteredRight = applyFilters(right, filters, (value: Int) => value < 50)
    filteredLeft
      .sortMergeJoin(filteredRight)
      .filter { case (_, (left, _)) => left > 10 }
      .map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
      .countByKey

  private def applyFilters(
      data: DistCollection[(Int, Int)],
      filters: Int,
      keepValue: Int => Boolean,
  ): DistCollection[(Int, Int)] =
    val keyed = data.filterKeys((key: Int) => key % 3 != 0)
    if (filters == 1) then keyed else keyed.filterValues(keepValue)

  private def time(
      name: String,
      plan: Plan[(Int, Int)],
      warmup: Int,
      rounds: Int,
      columnar: Boolean,
  ): Arm =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = columnar))
    Task.resetSortMergeJoinRunsForTest()
    HashCount.resetByKeyForTest()
    JoinWriteRuns.resetRunsForTest()
    val action = () => CrossEngineShapes.pairChecksum(DistCollection(plan).collect())
    var round = 0
    while (round < warmup) do
      ScarletRuntime.get.shuffle.clear()
      val _ = action()
      round += 1
    HashCount.resetByKeyForTest()
    JoinWriteRuns.resetRunsForTest()
    Task.resetSortMergeJoinRunsForTest()
    val samples = new Array[Double](rounds)
    var checksum = 0L
    var batches = 0L
    var fused = 0L
    round = 0
    while (round < rounds) do
      val sink = new CountingSink
      val previous = ColumnMetrics.install(sink)
      try
        ScarletRuntime.get.shuffle.clear()
        val started = System.nanoTime()
        checksum = action()
        samples(round) = (System.nanoTime() - started) / 1_000_000.0
        batches = sink.batchCount
        fused = sink.fusedCount
      finally ColumnMetrics.restore(previous)
      round += 1
    val steady = steadyOf(samples)
    java.util.Arrays.sort(samples)
    Arm(
      name,
      samples(samples.length / 2),
      steady.ms,
      steady.verdict,
      checksum,
      batches,
      fused,
      Task.sortMergeJoinRunsForTest,
      HashCount.byKeyCallsForTest,
      JoinWriteRuns.runsForTest,
    )

  private final case class Steady(ms: Double, verdict: String)

  /** Last 10 rounds. Fewer than 20 rounds is `short`. Halves more than 10% apart are `machine`. */
  private def steadyOf(samples: Array[Double]): Steady =
    val n = samples.length
    val tailN = math.min(10, n)
    if (n < 20) then Steady(medianRange(samples, n - tailN, n), "short")
    else
      val mid = n - tailN / 2
      val left = medianRange(samples, n - tailN, mid)
      val right = medianRange(samples, mid, n)
      val low = math.min(left, right)
      val high = math.max(left, right)
      val verdict = if (high > low * 1.10) then "machine" else "ok"
      Steady(medianRange(samples, n - tailN, n), verdict)

  private def medianRange(samples: Array[Double], from: Int, until: Int): Double =
    val copy = java.util.Arrays.copyOfRange(samples, from, until)
    java.util.Arrays.sort(copy)
    copy(copy.length / 2)

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

  private def render(arm: Arm): String =
    s"${arm.name} p50_ms=${arm.p50Ms} steady_ms=${arm.steadyMs} steady=${arm.steady} " +
      s"checksum=${arm.checksum} batches=${arm.batches} fused=${arm.fused} " +
      s"sort_merge_tasks=${arm.sortMergeTasks} by_key_calls=${arm.byKeyCalls} " +
      s"join_write_runs=${arm.joinWriteRuns}"

  private def readInt(name: String, default: Int): Int =
    sys.env.get(name) match
      case None => default
      case Some(text) =>
        text.toIntOption match
          case Some(value) => value
          case None => refuse(s"$name must be an integer, found $text")

  private def readPositive(name: String, default: Int): Int =
    readInt(name, default) match
      case value if value > 0 => value
      case value => refuse(s"$name must be positive, found $value")

  private def readChoice(name: String, default: Int, allowed: Set[Int]): Int =
    readInt(name, default) match
      case value if allowed.contains(value) => value
      case value => refuse(s"$name must be one of ${allowed.mkString(", ")}, found $value")

  private def readMode(): String =
    sys.env.get("CROSS_MODE") match
      case None => "both"
      case Some("row") => "row"
      case Some("both") => "both"
      case Some(other) => refuse(s"CROSS_MODE must be row or both, found $other")

  private def refuse(message: String): Nothing =
    System.err.println(message)
    sys.exit(1)
