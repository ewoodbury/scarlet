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
 * Same-session row and columnar times for a generic `(Int, Int)` key change then `reduceByKey`.
 *
 * The generator is the one [[PairMapBench]] uses: keys are `row % 1024`, values are the
 * cross-engine LCG, two source partitions, then `key % 3 != 0` and `value >= 0`. The action is
 * `data.map { case (k, v) => (k % 100, v) }.reduceByKey(_ + _)`. [[PairMapBench]] stays
 * `countByKey` and is not this action. At 1e7 rows and two filters the checksum is
 * 23246404924.
 *
 * Spill is on for this process only. Warmup 80, rounds 30, and four threads are the defaults.
 * Allocation is the middle of five process-wide thread byte deltas, in decimal megabytes, after
 * the timed rounds. The row arm runs first. The checksum is [[CrossEngineShapes.pairChecksum]].
 *
 * {{{
 * CROSS_ROWS=10000000 CROSS_THREADS=4 CROSS_WARMUP=80 CROSS_ROUNDS=30 \
 *   CROSS_FILTERS=2 java -Xmx2g -cp "$CP" \
 *   com.ewoodbury.scarlet.execution.PairProjectReduceBench
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Var"))
object PairProjectReduceBench:

  private val groups = 1024
  private val seed = 0x12345678
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val allocRounds = 5
  private val expectedAt1e7 = 23246404924L

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
      val reduced = query(rows, filters)
      val shape = ColumnarPlanner
        .lower(reduced.plan)
        .map(_.shape)
        .getOrElse(refuse("pair project reduce did not lower"))
      println(s"arm $shape")
      val row = time("row", reduced.plan, warmup, rounds, columnar = false)
      val columnar = time("columnar", reduced.plan, warmup, rounds, columnar = true)
      if (row.checksum != columnar.checksum) then
        refuse(s"checksums differ row=${row.checksum} columnar=${columnar.checksum}")
      if (rows == 10000000 && filters == 2 && columnar.checksum != expectedAt1e7) then
        refuse(s"checksum ${columnar.checksum} != $expectedAt1e7")
      if (row.batches != 0L || row.fused != 0L) then
        refuse(s"row published batches=${row.batches} fused=${row.fused}")
      if (columnar.batches == 0L && columnar.fused == 0L) then
        refuse("columnar published no column batch and folded nothing")
      println(render(row))
      println(render(columnar))
    finally
      try scheduler.shutdown().unsafeRunSync()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  /** Filtered int pairs, then `(k % 100, v)`, then `reduceByKey(_ + _)`. */
  private def query(rows: Int, filters: Int): DistCollection[(Int, Int)] =
    val keys = Array.tabulate(rows)(row => row % groups)
    val values = lcgValues(rows)
    val data = DistCollection.ofIntPairs(keys, values, sourcePartitions)
    val keyed = data.filterKeys((key: Int) => key % 3 != 0)
    val filtered = if (filters == 1) then keyed else keyed.filterValues((value: Int) => value >= 0)
    filtered.map { case (key, value) => (key % 100, value) }.reduceByKey[Int, Int](_ + _)

  private def time(
      name: String,
      plan: Plan[(Int, Int)],
      warmup: Int,
      rounds: Int,
      columnar: Boolean,
  ): Arm =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = columnar))
    val action = () => CrossEngineShapes.pairChecksum(DistCollection(plan).collect())
    var round = 0
    while (round < warmup) do
      ScarletRuntime.get.shuffle.clear()
      val _ = action()
      round += 1
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
    java.util.Arrays.sort(samples)
    Arm(name, samples(samples.length / 2), checksum, sampleAlloc(action), batches, fused)

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

  private def readChoice(name: String, default: Int, allowed: Set[Int]): Int =
    readInt(name, default) match
      case value if allowed.contains(value) => value
      case value => refuse(s"$name must be one of ${allowed.mkString(", ")}, found $value")

  private def refuse(message: String): Nothing =
    System.err.println(message)
    sys.exit(1)
