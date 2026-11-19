package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory

import cats.effect.unsafe.implicits.global
import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, LongHashCount, LongPairProject}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Same-session row and columnar times for one long slot above a long-pair project, then
 * `countByKey`.
 *
 * The generator is [[LongPairMapBench]]: keys are `row % 1024`, values are that LCG stored as
 * longs. The action is `map { case (k, v) => (k % 100, v) }.filterKeys(_ < 50).countByKey`.
 * `CROSS_FILTERS=2` is `k % 3 != 0` then `v >= 0` before that map. The row arm runs first and its
 * checksum is the one the columnar arm must match. [[LongHashCount.byKey]] does not see the
 * filtered project, and the pair batch is not published.
 *
 * Spill is on for this process only. Warmup 80, rounds 30, and four threads are the defaults.
 *
 * {{{
 * CROSS_ROWS=10000000 CROSS_THREADS=4 CROSS_WARMUP=80 CROSS_ROUNDS=30 CROSS_FILTERS=2 \
 *   sbt -J-Xmx768m \
 *   'set LocalProject("scarlet-tests") / Test / fork := true' \
 *   'set LocalProject("scarlet-tests") / Test / javaOptions := Seq("-Xmx4g", "-XX:+UseG1GC")' \
 *   'scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.ProjectedLongCountBench'
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Var"))
object ProjectedLongCountBench:

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
      fused: Long,
  )

  def main(args: Array[String]): Unit =
    val rows = readInt("CROSS_ROWS", 10000000)
    val filters = readChoice("CROSS_FILTERS", 2, Set(0, 1, 2))
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
      val counted = query(rows, filters)
      val shape = ColumnarPlanner
        .lower(counted.plan)
        .map(_.shape)
        .getOrElse(refuse("filtered long project count did not lower"))
      val slot =
        shape.contains("CountByKey") && shape.contains("Filter") && shape.contains("Project")
      if (!slot) then refuse(s"shape is not a slot above a long project: $shape")
      println(s"arm $shape")
      println(s"maxMemory=${Runtime.getRuntime.maxMemory}")
      val row = time("row", counted.plan, warmup, rounds, columnar = false)
      println(render(row))
      Console.out.flush()
      ScarletRuntime.get.shuffle.clear()
      System.gc()
      LongPairProject.resetPublishedForTest()
      LongHashCount.resetByKeyForTest()
      val columnar = time("columnar", counted.plan, warmup, rounds, columnar = true)
      val byKeyCalls = LongHashCount.byKeyCallsForTest
      val published = LongPairProject.publishedForTest
      println(render(columnar))
      println(s"columnar byKeyCalls=$byKeyCalls projectPublished=$published")
      val ratio = if (columnar.p50Ms == 0.0) then 0.0 else row.p50Ms / columnar.p50Ms
      println(s"ratio row/columnar=$ratio")
      Console.out.flush()
      if (row.checksum != columnar.checksum) then
        refuse(s"checksums differ row=${row.checksum} columnar=${columnar.checksum}")
      if (row.batches != 0L || row.fused != 0L) then
        refuse(s"row published batches=${row.batches} fused=${row.fused}")
      if (columnar.fused == 0L) then refuse("columnar folded nothing")
      if (byKeyCalls != 0) then refuse(s"LongHashCount.byKey ran $byKeyCalls times")
      if (published != 0) then refuse(s"LongPairProject published $published batches")
    finally
      try scheduler.shutdown().unsafeRunSync()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  /**
   * Filtered long pairs, then `(k % 100, v)`, then `filterKeys(_ < 50)`, then `countByKey`. The
   * slot sits above the project. `filters` is the stack under the project.
   */
  private def query(rows: Int, filters: Int): DistCollection[(Long, Int)] =
    val keys = Array.tabulate(rows)(row => (row % groups).toLong)
    val values = lcgValues(rows)
    val data = DistCollection.ofLongPairs(keys, values, sourcePartitions)
    val filtered = filters match
      case 0 => data
      case 1 => data.filterKeys((key: Long) => key % 3L != 0L)
      case _ =>
        data
          .filterKeys((key: Long) => key % 3L != 0L)
          .filterValues((value: Long) => value >= 0L)
    filtered
      .map { case (key, value) => (key % 100L, value) }
      .filterKeys((key: Long) => key < 50L)
      .countByKey

  private def time(
      name: String,
      plan: Plan[(Long, Int)],
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
    var fused = 0L
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
        fused = sink.fusedCount
      finally ColumnMetrics.restore(previous)
      round += 1
    java.util.Arrays.sort(samples)
    Arm(name, samples(samples.length / 2), checksumValue, sampleAlloc(action), batches, fused)

  /** Order-independent. The same fold as [[LongPairMapBench]], on a long key. */
  private def checksum(rows: Iterable[(Long, Int)]): Long =
    rows.iterator.foldLeft(0L)((total, row) => total + row._1 * 31L + row._2.toLong)

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
      s"batches=${arm.batches} fused=${arm.fused}"

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
