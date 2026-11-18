package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory

import cats.effect.unsafe.implicits.global
import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Same-session row and columnar times for one `(String, Int)` key change.
 *
 * Keys are `(row % 1024).toString`. Values are the cross-engine LCG. The action is
 * `map { case (k, v) => (k + "!", v) }.countByKey`. Spill stays at the process default. Warmup
 * 80, rounds 30, and four threads are the defaults. The row arm runs first. A plan that does not
 * lower prints `arm row` and does not time a second arm.
 *
 * {{{
 * CROSS_ROWS=1000000 CROSS_THREADS=4 CROSS_WARMUP=80 CROSS_ROUNDS=30 \
 *   sbt -J-Xmx4g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.StringPairMapBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Var"))
object StringPairMapBench:

  private val groups = 1024
  private val seed = 0x12345678
  private val batchSize = 8192
  private val sourcePartitions = 4
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
        ),
      )
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      val counted = query(rows)
      val lowered = ColumnarPlanner.lower(counted.plan)
      val shape = lowered.map(_.shape).getOrElse("row")
      println(s"arm $shape")
      val row = time("row", counted.plan, warmup, rounds, columnar = false)
      if (lowered.isEmpty) then println(render(row))
      else
        val columnar = time("columnar", counted.plan, warmup, rounds, columnar = true)
        if (row.checksum != columnar.checksum) then
          refuse(s"checksums differ row=${row.checksum} columnar=${columnar.checksum}")
        if (row.batches != 0L || row.fused != 0L) then
          refuse(s"row published batches=${row.batches} fused=${row.fused}")
        println(render(row))
        println(render(columnar))
        val ratio = row.p50Ms / columnar.p50Ms
        println(f"ratio ${ratio}%.3f")
    finally
      try scheduler.shutdown().unsafeRunSync()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  /** String pairs, then `(k + "!", v)`, then `countByKey`. */
  private def query(rows: Int): DistCollection[(String, Int)] =
    val texts = Array.tabulate(groups)(group => group.toString)
    val values = lcgValues(rows)
    val pairs = new Array[(String, Int)](rows)
    var index = 0
    while (index < rows) do
      pairs(index) = (texts(index % groups), values(index))
      index += 1
    DistCollection(pairs.toSeq, sourcePartitions)
      .map { case (key, value) => (key + "!", value) }
      .countByKey

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

  /** Order-independent. `String.hashCode` is the key. */
  private def checksum(rows: Iterable[(String, Int)]): Long =
    rows.iterator.foldLeft(0L) { (total, row) =>
      total + ((row._1.hashCode.toLong * 31L) ^ row._2.toLong)
    }

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

  private def refuse(message: String): Nothing =
    System.err.println(message)
    sys.exit(1)
