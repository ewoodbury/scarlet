package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.{Arrays, Locale}

import cats.effect.unsafe.implicits.global
import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.FoldedCount
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Phase split for int `filter-countByKey`. Not a test, and not part of `make test`.
 *
 * The input matches [[CrossEngineBench]]: keys `row % 1024`, the same LCG values, two source
 * partitions, batch 8192, shuffle width 4, two filters, spill on, 4 threads. Warmup and rounds
 * follow `CROSS_WARMUP` and `CROSS_ROUNDS` (80 and 30).
 *
 * Insert, merge, exchange, and encode are wall-clock pieces of the action. Insert folds each
 * worker stripe into one table. Merge combines those stripe partials. Exchange partitions the
 * combined keys. Encode is the post-exchange combine, which writes the survivor key and count
 * arrays. The four pieces are not subtracted from the action. `other` is the action minus those
 * four, on the steady medians, and is where `collect` decodes the pairs.
 *
 * {{{
 * CROSS_ROWS=10000000 java -cp … com.ewoodbury.scarlet.execution.CountByKeySplit
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object CountByKeySplit:

  private val groups = 1024
  private val seed = 0x12345678
  private val threads = 4
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val width = 4
  private val allocRounds = 5

  private final class HarnessRefusal(message: String) extends RuntimeException(message)

  def main(args: Array[String]): Unit =
    val rows = readRows()
    val warmup = readInt("CROSS_WARMUP", 80)
    val rounds = readInt("CROSS_ROUNDS", 30)
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(threads)
    var status = 0
    try
      ScarletConf.set(
        savedConf.copy(
          threadPoolSize = threads,
          columnarExecution = true,
          columnarBatchSize = batchSize,
          defaultShufflePartitions = width,
          spillEnabled = true,
        ),
      )
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      run(rows, warmup, rounds)
    catch
      case refusal: HarnessRefusal =>
        System.err.println(refusal.getMessage)
        status = 1
    finally
      try scheduler.shutdown().unsafeRunSync()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)
    if (status != 0) then sys.exit(status)

  private def run(rows: Int, warmup: Int, rounds: Int): Unit =
    val values = lcgInts(rows)
    val keys = Array.tabulate(rows)(row => row % groups)
    val raw = DistCollection.ofIntPairs(keys, values, sourcePartitions)
    val counted = raw
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .countByKey
    val action = () =>
      counted.collect().map { case (key, count) => key.toLong * 31L + count.toLong }.sum
    val expected = expectedChecksum(rows)
    var round = 0
    while (round < warmup) do
      ScarletRuntime.get.shuffle.clear()
      val got = action()
      if (expected >= 0L && got != expected) then refuse(s"checksum $got != $expected")
      round += 1
    val actionMs = new Array[Double](rounds)
    val insertMs = new Array[Double](rounds)
    val mergeMs = new Array[Double](rounds)
    val exchangeMs = new Array[Double](rounds)
    val encodeMs = new Array[Double](rounds)
    val youngBefore = youngCollections()
    val pauseBefore = youngPauseMs()
    round = 0
    var checksum = 0L
    while (round < rounds) do
      ScarletRuntime.get.shuffle.clear()
      val started = System.nanoTime()
      checksum = action()
      actionMs(round) = millis(System.nanoTime() - started)
      if (expected >= 0L && checksum != expected) then refuse(s"checksum $checksum != $expected")
      insertMs(round) = millis(FoldedCount.insertNanos)
      mergeMs(round) = millis(FoldedCount.mergeNanos)
      exchangeMs(round) = millis(FoldedCount.exchangeNanos)
      encodeMs(round) = millis(FoldedCount.encodeNanos)
      round += 1
    val youngAfter = youngCollections()
    val pauseAfter = youngPauseMs()
    val alloc = allocMedian(action)
    println(line("action", rows, actionMs))
    println(line("insert", rows, insertMs))
    println(line("merge", rows, mergeMs))
    println(line("exchange", rows, exchangeMs))
    println(line("encode", rows, encodeMs))
    val actionSteady = steady(actionMs)
    val pieces =
      steady(insertMs) + steady(mergeMs) + steady(exchangeMs) + steady(encodeMs)
    println(
      s"rows=$rows checksum=$checksum alloc_mb=${fmt(alloc)} " +
        s"young=${youngAfter - youngBefore} young_pause_ms=${pauseAfter - pauseBefore} " +
        s"stripes=${FoldedCount.stripesForTest} partials=${FoldedCount.partialsForTest} " +
        s"dense=${FoldedCount.denseSegmentsForTest} other_ms=${fmt(actionSteady - pieces)}",
    )

  private def allocMedian(action: () => Long): Double =
    threadBean match
      case None => Double.NaN
      case Some(bean) =>
        bean.setThreadAllocatedMemoryEnabled(true)
        val samples = new Array[Double](allocRounds)
        var round = 0
        while (round < allocRounds) do
          val before = allocated(bean)
          ScarletRuntime.get.shuffle.clear()
          val _ = action()
          val after = allocated(bean)
          samples(round) = (after - before).toDouble / 1_000_000.0
          round += 1
        median(samples)

  private def allocated(bean: ThreadMXBean): Long =
    val ids = bean.getAllThreadIds
    var index = 0
    var total = 0L
    while (index < ids.length) do
      val bytes = bean.getThreadAllocatedBytes(ids(index))
      if (bytes > 0L) then total += bytes
      index += 1
    total

  private def threadBean: Option[ThreadMXBean] =
    ManagementFactory.getThreadMXBean match
      case bean: ThreadMXBean => Some(bean)
      case _ => None

  private def youngCollections(): Long = youngTotal(_.getCollectionCount)

  private def youngPauseMs(): Long = youngTotal(_.getCollectionTime)

  private def youngTotal(read: java.lang.management.GarbageCollectorMXBean => Long): Long =
    val beans = ManagementFactory.getGarbageCollectorMXBeans
    var index = 0
    var total = 0L
    while (index < beans.size) do
      val bean = beans.get(index)
      val name = bean.getName
      if (name.indexOf("Young") >= 0 || name.indexOf("young") >= 0) then
        val count = read(bean)
        if (count > 0L) then total += count
      index += 1
    total

  private def expectedChecksum(rows: Int): Long =
    rows match
      case 1000000 => 11147169L
      case 10000000 => 14142272L
      case _ => -1L

  private def lcgInts(count: Int): Array[Int] =
    val generated = new Array[Int](count)
    var index = 0
    var state = seed
    while (index < count) do
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      generated(index) = (mixed % 2000001) - 1000000
      index += 1
    generated

  /** Upper median, the same slot [[CrossEngineBench]] publishes as `p50_ms`. */
  private def median(samples: Array[Double]): Double =
    val copy = Arrays.copyOf(samples, samples.length)
    Arrays.sort(copy)
    copy(copy.length / 2)

  /**
   * Upper median of the last 10 rounds. `machine` when the two halves of that tail differ by
   * more than 10%. `compilation` when the first 10 are still above the tail. Same rule as
   * [[CrossEngineBench]].
   */
  private def steady(samples: Array[Double]): Double =
    val from = samples.length - tailCount(samples)
    medianRange(samples, from, samples.length)

  private def verdict(samples: Array[Double]): String =
    val n = samples.length
    val tailN = tailCount(samples)
    if (n < 20) then "short"
    else
      val mid = n - tailN / 2
      val left = medianRange(samples, n - tailN, mid)
      val right = medianRange(samples, mid, n)
      val low = math.min(left, right)
      val high = math.max(left, right)
      val head = medianRange(samples, 0, math.min(10, n))
      if (high > low * 1.10) then "machine"
      else if (head > steady(samples) * 1.10) then "compilation"
      else "ok"

  private def tailCount(samples: Array[Double]): Int = math.min(10, samples.length)

  /** Upper median of `[from, until)`. */
  private def medianRange(samples: Array[Double], from: Int, until: Int): Double =
    val copy = Arrays.copyOfRange(samples, from, until)
    Arrays.sort(copy)
    copy(copy.length / 2)

  private def millis(nanos: Long): Double = nanos / 1_000_000.0

  private def line(phase: String, rows: Int, samples: Array[Double]): String =
    val p50 = fmt(median(samples))
    val steadyMs = fmt(steady(samples))
    s"phase=$phase rows=$rows p50_ms=$p50 steady_ms=$steadyMs steady=${verdict(samples)}"

  private def fmt(value: Double): String =
    String.format(Locale.ENGLISH, "%.3f", value)

  private def readRows(): Int =
    val rows = readInt("CROSS_ROWS", 1000000)
    if (rows > 0) then rows else refuse(s"CROSS_ROWS must be positive, found $rows")

  private def readInt(name: String, default: Int): Int =
    sys.env.get(name) match
      case None => default
      case Some(text) =>
        text.toIntOption.getOrElse(refuse(s"$name must be an integer, found $text"))

  private def refuse(message: String): Nothing =
    throw new HarnessRefusal(message)
