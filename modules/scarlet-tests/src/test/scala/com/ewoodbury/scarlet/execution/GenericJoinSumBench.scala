package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicInteger

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.{Partition, Plan, RetryPolicy, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.api.{RunnableTask, TaskScheduler}
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Same-session times for a generic join map and `reduceByKey(_ + _)`.
 *
 * The inputs match [[CrossEngineBench]] `filter-joinagg`: left keys `row % 1024`, the shared LCG
 * values, right keys `row % 1024` over `10 * 1024` rows with values `row % 100`. Two filters. The
 * action is the hand-written `(key, left.toLong)` map and `_ + _`, not `sumJoinLeft`.
 *
 * `CROSS_MODE=row` times only the row path. `CROSS_MODE=both` times the row path, then the
 * columnar path, in one JVM. Spill is on for this process. Warmup defaults to 80 and rounds to 30.
 * `CROSS_JOIN=hash` is the unhinted join. `CROSS_JOIN=sort-merge` is
 * `filteredLeft.sortMergeJoin(filteredRight)`. The default is `hash`.
 *
 * {{{
 * CROSS_ROWS=1000000 CROSS_FILTERS=2 CROSS_THREADS=4 CROSS_WARMUP=80 CROSS_ROUNDS=30 \
 *   CROSS_MODE=both sbt -J-Xmx4g \
 *   "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.GenericJoinSumBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Var"))
object GenericJoinSumBench:

  private val groups = 1024
  private val seed = 0x12345678
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val allocRounds = 5
  private val expectedAt1e6 = 836270016740L

  private final case class Arm(
      name: String,
      p50Ms: Double,
      steadyMs: Double,
      steady: String,
      checksum: Long,
      allocMb: Double,
      batches: Long,
      fused: Long,
  )

  def main(args: Array[String]): Unit =
    val rows = readPositive("CROSS_ROWS", 1000000)
    val filters = readChoice("CROSS_FILTERS", 2, Set(1, 2))
    val threads = readPositive("CROSS_THREADS", 4)
    val warmup = readPositive("CROSS_WARMUP", 80)
    val rounds = readPositive("CROSS_ROUNDS", 30)
    val mode = readMode()
    val join = readJoin()
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
      val summed = query(rows, filters, join)
      val shape = ColumnarPlanner.lower(summed.plan).map(_.shape).getOrElse("row")
      println(s"arm $shape")
      println(s"sort_merge ${shape.contains("SortMergeJoin")}")
      val row = time("row", summed.plan, warmup, rounds, columnar = false)
      if (row.batches != 0L || row.fused != 0L) then
        refuse(s"row published batches=${row.batches} fused=${row.fused}")
      if (rows == 1000000 && filters == 2 && row.checksum != expectedAt1e6) then
        refuse(s"row checksum ${row.checksum} != $expectedAt1e6")
      println(render(row))
      mode match
        case "both" =>
          val declined = shape match
            case "row" => true
            case other => other.contains("SortMergeJoin")
          if (declined) then println("columnar declined")
          else
            val columnar = time("columnar", summed.plan, warmup, rounds, columnar = true)
            if (row.checksum != columnar.checksum) then
              refuse(s"checksums differ row=${row.checksum} columnar=${columnar.checksum}")
            if (columnar.batches == 0L && columnar.fused == 0L) then
              refuse("columnar published no column batch and folded nothing")
            val ratio = row.p50Ms / columnar.p50Ms
            println(render(columnar))
            println(f"ratio_row_over_columnar $ratio%.3f")
            val matches = if (shape.contains("JoinProbeSum")) "not_written" else "written"
            println(s"match_columns $matches")
            println(s"sort_merge_task ${sortMergeTasks(summed.plan)}")
        case _ => ()
    finally
      try scheduler.shutdown().unsafeRunSync()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  /**
   * Filtered int join, then `(key, leftValue.toLong)` and `reduceByKey(_ + _)`. The lambda is not
   * [[com.ewoodbury.scarlet.api.LongSum]] and the map is not `sumJoinLeft`.
   */
  private def query(rows: Int, filters: Int, join: String): DistCollection[(Int, Long)] =
    val width = 10 * groups
    val joinKeys = Array.tabulate(width)(row => row % groups)
    val joinValues = Array.tabulate(width)(row => row % 100)
    val keys = Array.tabulate(rows)(row => row % groups)
    val values = lcgValues(rows)
    val left = DistCollection.ofIntPairs(keys, values, sourcePartitions)
    val right = DistCollection.ofIntPairs(joinKeys, joinValues, sourcePartitions)
    val filteredLeft = applyFilters(left, filters, (value: Int) => value >= 0)
    val filteredRight = applyFilters(right, filters, (value: Int) => value < 50)
    val joined = join match
      case "hash" => filteredLeft.join(filteredRight)
      case "sort-merge" => filteredLeft.sortMergeJoin(filteredRight)
      case other => refuse(s"CROSS_JOIN must be hash or sort-merge, found $other")
    joined
      .map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
      .reduceByKey((left: Long, right: Long) => left + right)

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
    val steady = steadyOf(samples)
    java.util.Arrays.sort(samples)
    Arm(
      name,
      samples(samples.length / 2),
      steady.ms,
      steady.verdict,
      checksum,
      sampleAlloc(action),
      batches,
      fused,
    )

  private final case class Steady(ms: Double, verdict: String)

  /** Last 10 rounds. Fewer than 20 rounds is `short`. Halves more than 10% apart are `machine`. */
  private def steadyOf(samples: Array[Double]): Steady =
    val n = samples.length
    val tailN = math.min(10, n)
    if (n < 20) then Steady(medianRange(samples, n - tailN, n), "short")
    else
      val mid = n - tailN / 2
      val left = medianRange(samples, n - tailN, mid)
      val right = medianRange(samples, mid, n)
      val low = math.min(left, right)
      val high = math.max(left, right)
      val verdict = if (high > low * 1.10) then "machine" else "ok"
      Steady(medianRange(samples, n - tailN, n), verdict)

  private def medianRange(samples: Array[Double], from: Int, until: Int): Double =
    val copy = java.util.Arrays.copyOfRange(samples, from, until)
    java.util.Arrays.sort(copy)
    copy(copy.length / 2)

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
    s"${arm.name} p50_ms=${arm.p50Ms} steady_ms=${arm.steadyMs} steady=${arm.steady} " +
      s"checksum=${arm.checksum} alloc_mb=${arm.allocMb} batches=${arm.batches} fused=${arm.fused}"

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

  /**
   * One columnar collect. Counts [[com.ewoodbury.scarlet.execution.Task.SortMergeJoinTask]]
   * submissions. This is not part of the timed rounds.
   */
  private def sortMergeTasks(plan: Plan[(Int, Long)]): Int =
    val counter = new AtomicInteger
    val saved = ScarletRuntime.get
    val inner = saved.scheduler
    val probe = new TaskScheduler[IO]:
      override def submit[A, B](tasks: Seq[RunnableTask[A, B]]): IO[Seq[Partition[B]]] =
        note(tasks, counter)
        inner.submit(tasks)
      override def submitWithRetry[A, B](
          tasks: Seq[RunnableTask[A, B]],
          policy: RetryPolicy,
      ): IO[Seq[Partition[B]]] =
        note(tasks, counter)
        inner.submitWithRetry(tasks, policy)
      override def shutdown(): IO[Unit] = IO.unit
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    ScarletRuntime.set(saved.copy(scheduler = probe))
    try
      ScarletRuntime.get.shuffle.clear()
      val _ = CrossEngineShapes.sumChecksum(DistCollection(plan).collect())
      counter.get
    finally ScarletRuntime.set(saved)

  private def note[A, B](tasks: Seq[RunnableTask[A, B]], counter: AtomicInteger): Unit =
    tasks.foreach { task =>
      if (task.getClass.getName.contains("SortMergeJoinTask")) then
        val _ = counter.incrementAndGet()
    }

  private def readJoin(): String =
    sys.env.get("CROSS_JOIN") match
      case None => "hash"
      case Some("hash") => "hash"
      case Some("sort-merge") => "sort-merge"
      case Some(other) => refuse(s"CROSS_JOIN must be hash or sort-merge, found $other")

  private def readMode(): String =
    sys.env.get("CROSS_MODE") match
      case None => "both"
      case Some("row") => "row"
      case Some("both") => "both"
      case Some(other) => refuse(s"CROSS_MODE must be row or both, found $other")

  private def refuse(message: String): Nothing =
    System.err.println(message)
    sys.exit(1)
