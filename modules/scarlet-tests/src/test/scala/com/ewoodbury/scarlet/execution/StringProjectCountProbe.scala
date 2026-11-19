package com.ewoodbury.scarlet.execution

import cats.effect.unsafe.implicits.global

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, StringPairProject}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Same-session times for `map { case (k, v) => (k + "!", v) }.countByKey`.
 *
 * The generator matches [[StringPairCountBench]] and does not apply that bench's `filterKeys`. Row
 * runs first. Spill stays off. Warmup 80, rounds 30, four threads, 1e6 rows.
 *
 * {{{
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.StringProjectCountProbe"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Var"))
object StringProjectCountProbe:

  private val groups = 1024
  private val seed = 0x12345678
  private val batchSize = 8192
  private val sourcePartitions = 2

  def main(args: Array[String]): Unit =
    val rows = sys.env.get("CROSS_ROWS").flatMap(_.toIntOption).getOrElse(1000000)
    val threads = sys.env.get("CROSS_THREADS").flatMap(_.toIntOption).getOrElse(4)
    val warmup = sys.env.get("CROSS_WARMUP").flatMap(_.toIntOption).getOrElse(80)
    val rounds = sys.env.get("CROSS_ROUNDS").flatMap(_.toIntOption).getOrElse(30)
    val columnarOnly = sys.env.get("CROSS_COLUMNAR_ONLY").contains("1")
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
      val counted = query(rows)
      val shape = ColumnarPlanner
        .lower(counted.plan)
        .map(_.shape)
        .getOrElse("none")
      println(s"arm $shape")
      val row =
        if (columnarOnly) then Arm("row", 0.0, 0L)
        else time("row", counted.plan, warmup, rounds, columnar = false)
      val columnar = time("columnar", counted.plan, warmup, rounds, columnar = true)
      StringPairProject.resetPublishedForTest()
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
      ScarletRuntime.get.shuffle.clear()
      val check = checksum(DistCollection(counted.plan).collect())
      val published = StringPairProject.publishedForTest
      println(s"publishedForTest=$published checksum=$check")
      println(render(row))
      println(render(columnar))
      println(s"ratio ${row.p50Ms / columnar.p50Ms}")
      if (!columnarOnly && (row.checksum != columnar.checksum || columnar.checksum != check)) then
        System.err.println(
          s"checksums row=${row.checksum} columnar=${columnar.checksum} check=$check",
        )
        sys.exit(1)
      if (columnarOnly && columnar.checksum != check) then
        System.err.println(s"checksums columnar=${columnar.checksum} check=$check")
        sys.exit(1)
    finally
      try scheduler.shutdown().unsafeRunSync()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  private def query(rows: Int): DistCollection[(String, Int)] =
    val pool = Array.tabulate(groups)(_.toString)
    val keys = Array.tabulate(rows)(row => pool(row % groups))
    val values = lcgValues(rows)
    DistCollection(keys.zip(values).toSeq, sourcePartitions).map { case (key, value) =>
      (key + "!", value)
    }.countByKey

  private final case class Arm(name: String, p50Ms: Double, checksum: Long)

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
    round = 0
    while (round < rounds) do
      val sink = new CountingSink
      val previous = ColumnMetrics.install(sink)
      try
        ScarletRuntime.get.shuffle.clear()
        val started = System.nanoTime()
        checksumValue = action()
        samples(round) = (System.nanoTime() - started) / 1_000_000.0
      finally ColumnMetrics.restore(previous)
      round += 1
    java.util.Arrays.sort(samples)
    Arm(name, samples(samples.length / 2), checksumValue)

  private def checksum(rows: Iterable[(String, Int)]): Long =
    var sum = 0L
    val iterator = rows.iterator
    while (iterator.hasNext) do
      val row = iterator.next()
      sum += row._1.hashCode.toLong * 31L + row._2.toLong
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
    s"${arm.name} p50_ms=${arm.p50Ms} checksum=${arm.checksum}"
