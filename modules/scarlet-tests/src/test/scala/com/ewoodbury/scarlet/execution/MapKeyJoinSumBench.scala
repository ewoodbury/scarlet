package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory

import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Same-session row and columnar times for an identity key map under a probe sum.
 *
 * The inputs are [[CrossEngineBench]]'s `filter-joinagg` tables: left keys `row % 1024`, the
 * shared LCG values, right keys `row % 1024` over `10 * 1024` rows with values `row % 100`. Two
 * filters. The action is `filteredLeft.mapIntKey(k => k).join(filteredRight).sumJoinLeft`.
 * Identity keeps checksum `836270016740` at 1e6 rows. The row arm runs first. A declined plan
 * prints the row sample and does not invent a columnar time.
 *
 * Spill is on for this process only. Warmup and rounds default to 20. Allocation is the middle
 * of five process-wide thread byte deltas, in decimal megabytes, after the timed rounds.
 *
 * {{{
 * CROSS_ROWS=1000000 CROSS_FILTERS=2 CROSS_THREADS=4 CROSS_WARMUP=20 CROSS_ROUNDS=20 \
 *   sbt -J-Xmx4g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.MapKeyJoinSumBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Var"))
object MapKeyJoinSumBench:

  private val groups = 1024
  private val seed = 0x12345678
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val allocRounds = 5
  private val expectedAt1e6 = 836270016740L

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
    val filters = readChoice("CROSS_FILTERS", 2, Set(1, 2))
    val threads = readPositive("CROSS_THREADS", 4)
    val warmup = readPositive("CROSS_WARMUP", 20)
    val rounds = readPositive("CROSS_ROUNDS", 20)
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
      val summed = query(rows, filters)
      val shape = ColumnarPlanner.lower(summed.plan).map(_.shape).getOrElse("row")
      val wroteMatches = if (shape.contains("JoinProbeSum")) "not_written" else "n/a"
      println(s"arm $shape")
      println(s"match_columns $wroteMatches")
      val row = time("row", summed.plan, warmup, rounds, columnar = false)
      if (row.batches != 0L || row.fused != 0L) then
        refuse(s"row published batches=${row.batches} fused=${row.fused}")
      if (rows == 1000000 && filters == 2 && row.checksum != expectedAt1e6) then
        refuse(s"row checksum ${row.checksum} != $expectedAt1e6")
      println(render(row))
      if (shape.contains("JoinProbeSum")) then
        val columnar = time("columnar", summed.plan, warmup, rounds, columnar = true)
        if (row.checksum != columnar.checksum) then
          refuse(s"checksums differ row=${row.checksum} columnar=${columnar.checksum}")
        if (columnar.batches == 0L && columnar.fused == 0L) then
          refuse("columnar published no column batch and folded nothing")
        val ratio = row.p50Ms / columnar.p50Ms
        println(render(columnar))
        println(f"ratio_row_over_columnar $ratio%.3f")
      else println("columnar declined")
    finally
      try scheduler.shutdown()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  /**
   * `joinAgg` inputs, then an identity `mapIntKey` on the left. The function is not inspected.
   * Two filters keep `value >= 0` on the left and `value < 50` on the right.
   */
  private def query(rows: Int, filters: Int): DistCollection[(Int, Long)] =
    val width = 10 * groups
    val joinKeys = Array.tabulate(width)(row => row % groups)
    val joinValues = Array.tabulate(width)(row => row % 100)
    val keys = Array.tabulate(rows)(row => row % groups)
    val values = lcgValues(rows)
    val left = DistCollection.ofIntPairs(keys, values, sourcePartitions)
    val right = DistCollection.ofIntPairs(joinKeys, joinValues, sourcePartitions)
    val filteredLeft = applyFilters(left, filters, (value: Int) => value >= 0)
    val filteredRight = applyFilters(right, filters, (value: Int) => value < 50)
    filteredLeft.mapIntKey(key => key).join(filteredRight).sumJoinLeft

  private def applyFilters(
      data: DistCollection[(Int, Int)],
      filters: Int,
      keepValue: Int => Boolean,
  ): DistCollection[(Int, Int)] =
    val keyed = data.filterKeys((key: Int) => key % 3 != 0)
    if (filters == 1) then keyed else keyed.filterValues(keepValue)

  private def time(
      name: String,
      plan: Plan[(Int, Long)],
      warmup: Int,
      rounds: Int,
      columnar: Boolean,
  ): Arm =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = columnar))
    val action = () => CrossEngineShapes.sumChecksum(DistCollection(plan).collect())
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
