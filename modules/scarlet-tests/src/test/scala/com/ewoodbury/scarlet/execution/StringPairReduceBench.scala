package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory

import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, StringPairProject}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Same-session times for a remapped string key, one key slot, then `reduceByKey`.
 *
 * The generator is [[StringPairCountBench]]: keys are `(row % 1024).toString`, values are the
 * cross-engine LCG, two source partitions. The action is
 * `map { case (k, v) => (k + "!", v) }.filterKeys(_ != "!").reduceByKey(_ + _)`. No generated key
 * is `"!"`, so the filter keeps every row. Spill stays off. Warmup 80, rounds 30, and four
 * threads are the defaults. The row arm runs first. The checksum is the order-independent sum of
 * `key.hashCode.toLong * 31 + wrapped value`.
 *
 * The published arm is the interpret path, forced for that collect: the filter writes the
 * projected pairs, then [[com.ewoodbury.scarlet.columnar.StringHashReduce.byKey]] reads them. The
 * fused arm inserts present projected pairs and does not publish that batch.
 *
 * {{{
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.StringPairReduceBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Var"))
object StringPairReduceBench:

  private val groups = 1024
  private val seed = 0x12345678
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val allocRounds = 5

  private final case class Arm(
      name: String,
      p50Ms: Double,
      checksum: Long,
      allocMb: Double,
      batches: Long,
      decoded: Long,
      published: Int,
  )

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
      val reduced = query(rows)
      val shape = ColumnarPlanner
        .lower(reduced.plan)
        .map(_.shape)
        .getOrElse(refuse("string pair reduce did not lower"))
      println(s"arm $shape")
      if (
        !shape.contains("StringHashAggregate") || !shape.contains("Filter") ||
        !shape.contains("Project") || shape.contains("row")
      ) then refuse(s"arm $shape is not StringHashAggregate of a Filter and a Project")
      val expected = expectedChecksum(rows)
      val row = time("row", reduced.plan, warmup, rounds, columnar = false, published = false)
      println(s"expected checksum=$expected")
      println(render(row))
      Console.out.flush()
      val published =
        time("published", reduced.plan, warmup, rounds, columnar = true, published = true)
      println(render(published))
      Console.out.flush()
      val fused = time("fused", reduced.plan, warmup, rounds, columnar = true, published = false)
      println(render(fused))
      val ratio = published.p50Ms / fused.p50Ms
      println(s"ratio published/fused=$ratio")
      Console.out.flush()
      if (row.checksum != expected) then
        refuse(s"row checksum ${row.checksum} != $expected")
      if (published.checksum != expected) then
        refuse(s"published checksum ${published.checksum} != $expected")
      if (fused.checksum != expected) then
        refuse(s"fused checksum ${fused.checksum} != $expected")
      if (row.batches != 0L || row.decoded != 0L) then
        refuse(s"row published batches=${row.batches} decoded=${row.decoded}")
      if (published.published == 0) then refuse("published arm did not publish the pair batch")
      if (fused.published != 0) then
        refuse(s"fused arm published ${fused.published} pair batches")
    finally
      try scheduler.shutdown()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  /** `(row % 1024).toString` with LCG values, then `(k + "!", v)`, drop `"!"`, then `reduceByKey`. */
  private def query(rows: Int): DistCollection[(String, Int)] =
    val pool = Array.tabulate(groups)(_.toString)
    val keys = Array.tabulate(rows)(row => pool(row % groups))
    val values = lcgValues(rows)
    DistCollection(keys.zip(values).toSeq, sourcePartitions)
      .map { case (key, value) => (key + "!", value) }
      .filterKeys((key: String) => key != "!")
      .reduceByKey[String, Int](_ + _)

  private def time(
      name: String,
      plan: Plan[(String, Int)],
      warmup: Int,
      rounds: Int,
      columnar: Boolean,
      published: Boolean,
  ): Arm =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = columnar))
    val action = () => checksum(run(plan, published))
    var round = 0
    while (round < warmup) do
      ScarletRuntime.get.shuffle.clear()
      val _ = action()
      round += 1
    val samples = new Array[Double](rounds)
    var checksumValue = 0L
    var batches = 0L
    var decoded = 0L
    var publishedCount = 0
    round = 0
    while (round < rounds) do
      val sink = new CountingSink
      val previous = ColumnMetrics.install(sink)
      StringPairProject.resetPublishedForTest()
      try
        ScarletRuntime.get.shuffle.clear()
        val started = System.nanoTime()
        checksumValue = action()
        samples(round) = (System.nanoTime() - started) / 1_000_000.0
        batches = sink.batchCount
        decoded = sink.decodedValueCount
        publishedCount = StringPairProject.publishedForTest
      finally ColumnMetrics.restore(previous)
      round += 1
    java.util.Arrays.sort(samples)
    Arm(
      name,
      samples(samples.length / 2),
      checksumValue,
      sampleAlloc(action),
      batches,
      decoded,
      publishedCount,
    )

  /** Collect. `published` forces the interpret filter that writes the pair batch. */
  private def run(plan: Plan[(String, Int)], published: Boolean): Iterable[(String, Int)] =
    if (published) then ReduceArms.withPublishedFilter(DistCollection(plan).collect())
    else DistCollection(plan).collect()

  private def checksum(rows: Iterable[(String, Int)]): Long =
    var sum = 0L
    val iterator = rows.iterator
    while (iterator.hasNext) do
      val row = iterator.next()
      sum += row._1.hashCode.toLong * 31L + row._2.toLong
    sum

  /**
   * Residue `r` occurs `rows / 1024` times, plus one when `r < rows % 1024`. The mapped key is
   * `r.toString + "!"`. The value sum wraps at `Int`, matching `reduceByKey`.
   */
  private def expectedChecksum(rows: Int): Long =
    val values = lcgValues(rows)
    val sums = new Array[Int](groups)
    var index = 0
    while (index < rows) do
      sums(index % groups) += values(index)
      index += 1
    var hash = 0L
    var residue = 0
    while (residue < groups) do
      val key = residue.toString + "!"
      hash += key.hashCode.toLong * 31L + sums(residue).toLong
      residue += 1
    hash

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

  /** Middle sample of [[allocRounds]] process-wide byte deltas, in decimal megabytes. */
  private def sampleAlloc(action: () => Long): Double =
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

  private def render(arm: Arm): String =
    s"${arm.name} p50_ms=${arm.p50Ms} checksum=${arm.checksum} alloc_mb=${arm.allocMb} " +
      s"batches=${arm.batches} decoded=${arm.decoded} published=${arm.published}"

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
