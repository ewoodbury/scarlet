package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Same-session row and columnar times for a long-pair project under `sortByLongKey`.
 *
 * The generator is [[LongPairMapBench]]: keys are `row % 1024`, values are that LCG stored as
 * longs, two source partitions. `CROSS_FILTERS=2` is `k % 3 != 0` then `v >= 0` before the map.
 * The action is `data.map { case (k, v) => (k % 100, v) }.sortByLongKey`. The checksum is ordered:
 * `hash * 31 + key` then `hash * 31 + value`, taken from the row path first. `countByKey` on this
 * generator is [[LongPairMapBench]] and is not this action.
 *
 * Spill is on for this process only. Warmup 80, rounds 30, and four threads are the defaults.
 * The row arm runs first. A plan that does not lower prints `arm row` and does not time a second
 * arm. `$CP` is the `scarlet-tests` test fullClasspath.
 *
 * {{{
 * CROSS_ROWS=10000000 CROSS_THREADS=4 CROSS_WARMUP=80 CROSS_ROUNDS=30 CROSS_FILTERS=2 \
 *   java -Xmx1536m -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError -cp "$CP" \
 *   com.ewoodbury.scarlet.execution.ProjectedLongSortBench
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object ProjectedLongSortBench:

  private val groups = 1024
  private val seed = 0x12345678
  private val batchSize = 8192
  private val sourcePartitions = 2

  private final case class Arm(name: String, p50Ms: Double, checksum: Long, batches: Long)

  def main(args: Array[String]): Unit =
    val rows = readPositive("CROSS_ROWS", 10000000)
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
      val sorted = query(rows, filters)
      val shape = ColumnarPlanner.lower(sorted.plan).map(_.shape).getOrElse("row")
      println(s"arm $shape")
      val row = time("row", sorted.plan, warmup, rounds, columnar = false)
      println(render(row))
      shape match
        case "row" => ()
        case _ =>
          val columnar = time("columnar", sorted.plan, warmup, rounds, columnar = true)
          if (row.checksum != columnar.checksum) then
            refuse(s"checksums differ row=${row.checksum} columnar=${columnar.checksum}")
          val ratio = row.p50Ms / columnar.p50Ms
          println(render(columnar))
          println("ratio " + java.lang.Double.toString(ratio))
    finally
      try scheduler.shutdown()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  /** Filtered long pairs, then `(k % 100, v)`, then `sortByLongKey`. */
  private def query(rows: Int, filters: Int): DistCollection[(Long, Long)] =
    val keys = Array.tabulate(rows)(row => (row % groups).toLong)
    val values = lcgValues(rows)
    val data = DistCollection.ofLongPairs(keys, values, sourcePartitions)
    val keyed = data.filterKeys((key: Long) => key % 3L != 0L)
    val filtered =
      if (filters == 1) then keyed else keyed.filterValues((value: Long) => value >= 0L)
    filtered.map { case (key, value) => (key % 100L, value) }.sortByLongKey

  private def time(
      name: String,
      plan: Plan[(Long, Long)],
      warmup: Int,
      rounds: Int,
      columnar: Boolean,
  ): Arm =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = columnar))
    val action = () => checksum(DistCollection(plan).collect())
    var round = 0
    while (round < warmup) do
      ScarletRuntime.get.shuffle.clear()
      val _ = action()
      round += 1
    val samples = new Array[Double](rounds)
    var checksumValue = 0L
    var batches = 0L
    round = 0
    while (round < rounds) do
      val sink = new CountingSink
      val previous = ColumnMetrics.install(sink)
      try
        ScarletRuntime.get.shuffle.clear()
        val started = System.nanoTime()
        checksumValue = action()
        samples(round) = (System.nanoTime() - started) / 1_000_000.0
        batches = sink.batchCount
      finally ColumnMetrics.restore(previous)
      round += 1
    java.util.Arrays.sort(samples)
    Arm(name, samples(samples.length / 2), checksumValue, batches)

  /** Order-sensitive. The row path is timed first, so this hash is that order. */
  private def checksum(rows: Iterable[(Long, Long)]): Long =
    val iterator = rows.iterator
    var hash = 0L
    while (iterator.hasNext) do
      val pair = iterator.next()
      hash = hash * 31L + pair._1
      hash = hash * 31L + pair._2
    hash

  private def lcgValues(count: Int): Array[Long] =
    val values = new Array[Long](count)
    var index = 0
    var state = seed
    while (index < count) do
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      values(index) = ((mixed % 2000001) - 1000000).toLong
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

  private def refuse(message: String): Nothing =
    System.err.println(message)
    sys.exit(1)
