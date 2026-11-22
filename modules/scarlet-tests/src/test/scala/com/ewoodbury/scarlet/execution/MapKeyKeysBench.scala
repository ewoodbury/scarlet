package com.ewoodbury.scarlet.execution


import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Same-session row and columnar times for `mapIntKey`, one `filterKeys`, then `keys.count`.
 *
 * The generator is the one [[PairProjectReduceBench]] uses: keys are `row % 1024`, values are the
 * cross-engine LCG, two source partitions, then `key % 3 != 0` and `value >= 0`. The action is
 * `data.mapIntKey(_ % 100).filterKeys(_ < 50).keys.count`. The count is taken from the row path
 * first. `% 100` is one hundred groups; the timed result is the surviving row count.
 *
 * Spill is on for this process only. Warmup 80, rounds 30, and four threads are the defaults.
 *
 * {{{
 * CROSS_ROWS=10000000 CROSS_THREADS=4 CROSS_WARMUP=80 CROSS_ROUNDS=30 \
 *   CROSS_FILTERS=2 java -Xmx4g -cp "$CP" \
 *   com.ewoodbury.scarlet.execution.MapKeyKeysBench
 * }}}
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.Var",
  ),
)
object MapKeyKeysBench:

  private val groups = 1024
  private val seed = 0x12345678
  private val batchSize = 8192
  private val sourcePartitions = 2

  private final case class Arm(
      name: String,
      p50Ms: Double,
      count: Long,
      batches: Long,
      fused: Long,
  )

  def main(args: Array[String]): Unit =
    val rows = readInt("CROSS_ROWS", 10000000)
    val filters = readChoice("CROSS_FILTERS", 2, Set(1, 2))
    val threads = readPositive("CROSS_THREADS", 4)
    val warmup = readPositive("CROSS_WARMUP", 80)
    val rounds = readPositive("CROSS_ROUNDS", 30)
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
      val keyed = query(rows, filters)
      val shape = ColumnarPlanner.lower(keyed.plan).map(_.shape).getOrElse("none")
      val folded = ColumnarPlanner.tryCount(keyed.plan)
      println(s"arm $shape")
      println(s"tryCount $folded")
      val row = time("row", keyed.plan, warmup, rounds, columnar = false)
      val columnar = time("columnar", keyed.plan, warmup, rounds, columnar = true)
      println(render(row))
      println(render(columnar))
      val ratio = if (columnar.p50Ms == 0.0) then 0.0 else row.p50Ms / columnar.p50Ms
      println(s"ratio row/columnar=$ratio")
      if (row.count != columnar.count) then
        refuse(s"counts differ row=${row.count} columnar=${columnar.count}")
      if (folded != Some(row.count)) then refuse(s"tryCount $folded != row count ${row.count}")
      if (!shape.startsWith("Key(") || !shape.contains("Filter") || !shape.contains("MapKey")) then
        refuse(s"arm $shape is not Key over Filter over MapKey")
      if (row.batches != 0L || row.fused != 0L) then
        refuse(s"row published batches=${row.batches} fused=${row.fused}")
    finally
      try scheduler.shutdown()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  /** Filtered int pairs, then `k % 100`, then `filterKeys(_ < 50)`, then `keys`. */
  private def query(rows: Int, filters: Int): DistCollection[Int] =
    val keys = Array.tabulate(rows)(row => row % groups)
    val values = lcgValues(rows)
    val data = DistCollection.ofIntPairs(keys, values, sourcePartitions)
    val keyed = data.filterKeys((key: Int) => key % 3 != 0)
    val filtered = if (filters == 1) then keyed else keyed.filterValues((value: Int) => value >= 0)
    filtered.mapIntKey(key => key % 100).filterKeys((key: Int) => key < 50).keys

  private def time(
      name: String,
      plan: Plan[Int],
      warmup: Int,
      rounds: Int,
      columnar: Boolean,
  ): Arm =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = columnar))
    val action = () => DistCollection(plan).count()
    var round = 0
    while (round < warmup) do
      ScarletRuntime.get.shuffle.clear()
      val _ = action()
      round += 1
    val samples = new Array[Double](rounds)
    var count = 0L
    var batches = 0L
    var fused = 0L
    round = 0
    while (round < rounds) do
      val sink = new CountingSink
      val previous = ColumnMetrics.install(sink)
      try
        ScarletRuntime.get.shuffle.clear()
        val started = System.nanoTime()
        count = action()
        samples(round) = (System.nanoTime() - started) / 1_000_000.0
        batches = sink.batchCount
        fused = sink.fusedCount
      finally ColumnMetrics.restore(previous)
      round += 1
    java.util.Arrays.sort(samples)
    Arm(name, samples(samples.length / 2), count, batches, fused)

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
    s"${arm.name} p50_ms=${arm.p50Ms} count=${arm.count} batches=${arm.batches} fused=${arm.fused}"

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

  private def refuse(message: String): Nothing =
    System.err.println(message)
    sys.exit(1)
