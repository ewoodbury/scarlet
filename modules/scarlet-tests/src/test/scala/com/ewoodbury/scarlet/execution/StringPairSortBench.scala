package com.ewoodbury.scarlet.execution


import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Same-session row and columnar times for a remapped string key under `sortByStringKey`.
 *
 * Keys are `(row % 1024).toString`. Values are the cross-engine LCG. Two source partitions. The
 * action is `map { case (k, v) => (k + "!", v) }.sortByStringKey`. Spill stays off. The default is
 * 1e6 rows. Warmup 80, rounds 30, and four threads are the defaults. The row arm runs first. The
 * checksum is the ordered mix of `key.hashCode` and the value, so a permutation does not match.
 *
 * {{{
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.StringPairSortBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Var"))
object StringPairSortBench:

  private val groups = 1024
  private val seed = 0x12345678
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val mix = 1000003L

  private final case class Arm(name: String, p50Ms: Double, checksum: Long, batches: Long)

  def main(args: Array[String]): Unit =
    val rows = readPositive("CROSS_ROWS", 1000000)
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
          spillEnabled = false,
        ),
      )
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      val sorted = query(rows)
      val shape = ColumnarPlanner.lower(sorted.plan).map(_.shape).getOrElse("row")
      println(s"arm $shape")
      if (
          !shape.contains("SortByStringKey") || !shape.contains("Project") || shape.contains("row")
        )
      then refuse(s"arm $shape is not SortByStringKey of a Project")
      val expected = expectedChecksum(rows)
      println(s"expected checksum=$expected")
      val row = time("row", sorted.plan, warmup, rounds, columnar = false)
      println(render(row))
      if (row.checksum != expected) then refuse(s"row checksum ${row.checksum} != $expected")
      if (row.batches != 0L) then refuse(s"row published batches=${row.batches}")
      val columnar = time("columnar", sorted.plan, warmup, rounds, columnar = true)
      val ratio = row.p50Ms / columnar.p50Ms
      println(render(columnar))
      println(s"ratio $ratio")
      if (row.checksum != columnar.checksum) then
        refuse(s"checksums differ row=${row.checksum} columnar=${columnar.checksum}")
      if (columnar.batches == 0L) then refuse("columnar published no column batch")
      // Map collect only. Its checksum is unsorted, so it is not the sort pair above.
      val project = time("project", queryProject(rows).plan, 10, 10, columnar = true)
      println(render(project))
      if (ratio < 2.0) then refuse(s"ratio $ratio is under 2")
    finally
      try scheduler.shutdown()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  /** `(row % 1024).toString` with LCG values, then `(k + "!", v)`, then `sortByStringKey`. */
  private def query(rows: Int): DistCollection[(String, Int)] =
    queryProject(rows).sortByStringKey

  private def queryProject(rows: Int): DistCollection[(String, Int)] =
    val pool = Array.tabulate(groups)(_.toString)
    val keys = Array.tabulate(rows)(row => pool(row % groups))
    val values = lcgValues(rows)
    DistCollection(keys.zip(values).toSeq, sourcePartitions).map { case (key, value) =>
      (key + "!", value)
    }

  private def time(
      name: String,
      plan: Plan[(String, Int)],
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

  private def checksum(rows: Iterable[(String, Int)]): Long =
    var sum = 0L
    val iterator = rows.iterator
    while (iterator.hasNext) do
      val row = iterator.next()
      sum = sum * mix + row._1.hashCode.toLong
      sum = sum * mix + row._2.toLong
    sum

  /**
   * Ordered mix of the mapped pairs. Ties break by input index, which is the contiguous
   * two-partition split: earlier records stay in earlier partitions.
   */
  private def expectedChecksum(rows: Int): Long =
    val pool = Array.tabulate(groups)(group => group.toString + "!")
    val keys = Array.tabulate(rows)(row => pool(row % groups))
    val values = lcgValues(rows)
    val order = Array.tabulate(rows)(identity)
    scala.util.Sorting.stableSort(
      order,
      (left: Int, right: Int) => {
        val compared = keys(left).compareTo(keys(right))
        if (compared != 0) then compared < 0 else left < right
      },
    )
    var sum = 0L
    var index = 0
    while (index < rows) do
      val at = order(index)
      sum = sum * mix + keys(at).hashCode.toLong
      sum = sum * mix + values(at).toLong
      index += 1
    sum

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
    sys.env.get(name) match
      case None => default
      case Some(text) =>
        text.toIntOption match
          case Some(value) if value > 0 => value
          case Some(value) => refuse(s"$name must be positive, found $value")
          case None => refuse(s"$name must be an integer, found $text")

  private def refuse(message: String): Nothing =
    System.err.println(message)
    sys.exit(1)
