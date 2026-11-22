package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, IntTopN}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Same-session row and columnar times for one int `mapValues` under `sortByIntValueDesc`.
 *
 * The generator matches [[ProjectedValueSortBench]]: keys are `row % 1024`, values are the shared
 * LCG, two source partitions, then `key % 3 != 0` and `value >= 0` when two filters are
 * requested. The action is `data.mapValues((v: Int) => v + 1).sortByIntValueDesc.take(100)`.
 * That bench's project query is not this action. The checksum is
 * [[CrossEngineShapes.pairChecksum]] of `take(100)`, taken from the row path first.
 *
 * Spill is on for this process only. Warmup 80, rounds 30, and four threads are the defaults.
 * The row arm runs first. `CROSS_ROW_ONLY=true` times only that arm.
 *
 * {{{
 * CROSS_ROWS=10000000 CROSS_THREADS=4 CROSS_WARMUP=80 CROSS_ROUNDS=30 CROSS_FILTERS=2 \
 *   java -Xmx4g -cp "$CP" com.ewoodbury.scarlet.execution.MappedValueSortBench
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object MappedValueSortBench:

  private val groups = 1024
  private val seed = 0x12345678
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val taken = 100

  private final case class Arm(name: String, p50Ms: Double, checksum: Long, batches: Long)

  def main(args: Array[String]): Unit =
    val rows = readPositive("CROSS_ROWS", 10000000)
    val filters = readChoice("CROSS_FILTERS", 2, Set(1, 2))
    val threads = readPositive("CROSS_THREADS", 4)
    val warmup = readPositive("CROSS_WARMUP", 80)
    val rounds = readPositive("CROSS_ROUNDS", 30)
    val rowOnly = readFlag("CROSS_ROW_ONLY", false)
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
      val sorted = query(rows, filters)
      val shape = ColumnarPlanner.lower(sorted.plan).map(_.shape).getOrElse("row")
      println(s"arm $shape")
      if (!rowOnly) then
        if (
            !shape.contains("SortByIntValueDesc") || !shape.contains("MapValue") ||
            shape.contains("row")
          )
        then refuse(s"arm $shape is not SortByIntValueDesc of a MapValue")
      val row = time("row", sorted.plan, warmup, rounds, columnar = false)
      println(render(row))
      if (!rowOnly) then
        IntTopN.resetBatchesForTest()
        val columnar = time("columnar", sorted.plan, warmup, rounds, columnar = true)
        if (row.checksum != columnar.checksum) then
          refuse(s"checksums differ row=${row.checksum} columnar=${columnar.checksum}")
        val ratio = row.p50Ms / columnar.p50Ms
        println(render(columnar))
        println("ratio " + java.lang.Double.toString(ratio))
        println(s"intTopNBatches ${IntTopN.batchCallsForTest}")
        if (IntTopN.batchCallsForTest <= 0) then refuse("IntTopN did not sort mapped batches")
        if (ratio < 2.0) then refuse(s"ratio $ratio is under 2")
    finally
      try scheduler.shutdown()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  /**
   * Filtered int pairs, then `mapValues(v + 1)`, then `sortByIntValueDesc`. `take(100)` is the
   * timed action. The function is not read by lowering.
   */
  private def query(rows: Int, filters: Int): DistCollection[(Int, Int)] =
    val keys = Array.tabulate(rows)(row => row % groups)
    val values = lcgValues(rows)
    val data = DistCollection.ofIntPairs(keys, values, sourcePartitions)
    val keyed = data.filterKeys((key: Int) => key % 3 != 0)
    val filtered = if (filters == 1) then keyed else keyed.filterValues((value: Int) => value >= 0)
    filtered.mapValues((value: Int) => value + 1).sortByIntValueDesc

  private def time(
      name: String,
      plan: Plan[(Int, Int)],
      warmup: Int,
      rounds: Int,
      columnar: Boolean,
  ): Arm =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = columnar))
    val action = () => CrossEngineShapes.pairChecksum(DistCollection(plan).take(taken))
    var round = 0
    while (round < warmup) do
      ScarletRuntime.get.shuffle.clear()
      val _ = action()
      round += 1
    val samples = new Array[Double](rounds)
    var checksum = 0L
    var batches = 0L
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
      finally ColumnMetrics.restore(previous)
      round += 1
    java.util.Arrays.sort(samples)
    Arm(name, samples(samples.length / 2), checksum, batches)

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
    s"${arm.name} p50_ms=${arm.p50Ms} checksum=${arm.checksum} batches=${arm.batches}"

  private def readPositive(name: String, default: Int): Int =
    readInt(name, default) match
      case value if value > 0 => value
      case value => refuse(s"$name must be positive, found $value")

  private def readChoice(name: String, default: Int, allowed: Set[Int]): Int =
    readInt(name, default) match
      case value if allowed.contains(value) => value
      case value => refuse(s"$name must be one of ${allowed.mkString(", ")}, found $value")

  private def readInt(name: String, default: Int): Int =
    sys.env.get(name) match
      case None => default
      case Some(text) =>
        text.toIntOption match
          case Some(value) => value
          case None => refuse(s"$name must be an integer, found $text")

  private def readFlag(name: String, default: Boolean): Boolean =
    sys.env.get(name) match
      case None => default
      case Some("true") => true
      case Some("false") => false
      case Some(text) => refuse(s"$name must be true or false, found $text")

  private def refuse(message: String): Nothing =
    System.err.println(message)
    sys.exit(1)
