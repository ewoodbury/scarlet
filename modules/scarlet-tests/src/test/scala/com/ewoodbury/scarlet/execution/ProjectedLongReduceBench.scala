package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory

import cats.effect.unsafe.implicits.global
import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{
  ColumnMetrics,
  CountingSink,
  LongHashReduce,
  LongPairProject,
}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Same-session row and columnar times for one long-pair project then `reduceByKey`.
 *
 * The generator is [[LongPairMapBench]]: keys are `row % 1024`, values are that LCG stored as
 * longs. The action is `map { case (k, v) => (k % 100, v) }.reduceByKey(_ + _)`.
 * `CROSS_FILTERS=2` is `k % 3 != 0` then `v >= 0` before that map. The row arm runs first and
 * its checksum is the one the columnar arm must match.
 *
 * Spill is on for this process only. Warmup 80, rounds 30, and four threads are the defaults.
 *
 * {{{
 * CROSS_ROWS=10000000 CROSS_THREADS=4 CROSS_WARMUP=80 CROSS_ROUNDS=30 CROSS_FILTERS=0 \
 *   sbt -J-Xmx768m \
 *   'set LocalProject("scarlet-tests") / Test / fork := true' \
 *   'set LocalProject("scarlet-tests") / Test / javaOptions := Seq("-Xmx4g", "-XX:+UseG1GC")' \
 *   'scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.ProjectedLongReduceBench'
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Var"))
object ProjectedLongReduceBench:

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
      val reduced = query(rows, filters)
      val lowered = ColumnarPlanner.lower(reduced.plan)
      val shape = lowered.map(_.shape).getOrElse("row")
      println(s"arm $shape")
      println(s"maxMemory=${Runtime.getRuntime.maxMemory}")
      // One row collect is the checksum. Timing that arm for 110 rounds of 1e7 boxed pairs
      // exhausts this heap before the columnar arm is measured. CROSS_SKIP_ROW=1 reuses a
      // checksum already taken in this session.
      val rowChecksum =
        if (sys.env.get("CROSS_SKIP_ROW").contains("1")) then readLong("CROSS_CHECKSUM", 0L)
        else
          ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
          ScarletRuntime.get.shuffle.clear()
          val found = checksum(DistCollection(reduced.plan).collect())
          System.gc()
          found
      println(s"row checksum=$rowChecksum")
      if (lowered.isEmpty) then ()
      else
        val columnar = time("columnar", reduced.plan, warmup, rounds, columnar = true)
        if (rowChecksum != columnar.checksum) then
          refuse(s"checksums differ row=$rowChecksum columnar=${columnar.checksum}")
        if (columnar.batches == 0L && columnar.fused == 0L) then
          refuse("columnar published no column batch and folded nothing")
        println(render(columnar))
        probePublished(reduced.plan, rowChecksum)
    finally
      try scheduler.shutdown().unsafeRunSync()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  /** Long pairs, optional key and value filters, then `(k % 100, v)`, then `reduceByKey`. */
  private def query(rows: Int, filters: Int): DistCollection[(Long, Long)] =
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
    filtered.map { case (key, value) => (key % 100L, value) }.reduceByKey[Long, Long](_ + _)

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

  /** One collect outside the timer. The project publish counter is the pair-batch signal. */
  private def probePublished(plan: Plan[(Long, Long)], rowChecksum: Long): Unit =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()
    LongPairProject.resetPublishedForTest()
    LongHashReduce.resetByKeyForTest()
    val found = checksum(DistCollection(plan).collect())
    if (found != rowChecksum) then
      refuse(s"probe checksum=$found row=$rowChecksum")
    println(
      s"probe checksum=$found published=${LongPairProject.publishedForTest} " +
        s"byKey=${LongHashReduce.byKeyCallsForTest}",
    )

  /** Order-independent. The same fold as [[LongPairMapBench]], on a long value. */
  private def checksum(rows: Iterable[(Long, Long)]): Long =
    rows.iterator.foldLeft(0L)((total, row) => total + row._1 * 31L + row._2)

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

  private def readLong(name: String, default: Long): Long =
    sys.env.get(name) match
      case None => default
      case Some(text) =>
        text.toLongOption match
          case Some(value) => value
          case None => refuse(s"$name must be an integer, found $text")

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
