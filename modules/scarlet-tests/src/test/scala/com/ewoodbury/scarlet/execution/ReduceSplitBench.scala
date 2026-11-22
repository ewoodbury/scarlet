package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.{Arrays, Locale}

import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Phase split for the long `filter-reducebykey` action. Not a test, and not part of `make test`.
 *
 * The input matches [[CrossEngineBench]]: keys `row % 1024`, the same LCG values, two source
 * partitions, batch 8192, shuffle width 4, two filters, spill on. Warmup and rounds follow
 * `CROSS_WARMUP` and `CROSS_ROUNDS` (80 and 30). Insert, combine, and exchange are wall-clock
 * pieces of that action. Share and decode are timed on their own, not subtracted from the action.
 *
 * {{{
 * CROSS_ROWS=1000000 java -cp … com.ewoodbury.scarlet.execution.ReduceSplitBench
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object ReduceSplitBench:

  private val groups = 1024
  private val seed = 0x12345678
  private val threads = 4
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val width = 4
  private val allocRounds = 5

  private val keepKey: ColumnKernel.Int64Predicate = (key: Long) => key % 3L != 0L
  private val keepValue: ColumnKernel.Int64Predicate = (value: Long) => value >= 0L
  private val steps = List(Selection.Step.Int64(0, keepKey), Selection.Step.Int64(1, keepValue))
  private val plus: LongHashReduce.LongBinOp = (left: Long, right: Long) => left + right

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
      try scheduler.shutdown()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)
    if (status != 0) then sys.exit(status)

  private def run(rows: Int, warmup: Int, rounds: Int): Unit =
    val values = lcgLongs(rows)
    val keys = Array.tabulate(rows)(row => (row % groups).toLong)
    val raw = DistCollection.ofLongPairs(keys, values, sourcePartitions)
    val parts = sourcePartitionsOf(raw.plan)
    val filtered = raw
      .filterKeys((key: Long) => key % 3L != 0L)
      .filterValues((value: Long) => value >= 0L)
    val reduced = filtered.reduceByKey[Long, Long](_ + _)
    val action = () => {
      ScarletRuntime.get.shuffle.clear()
      reduced.collect().map { case (key, value) => key * 31L + value }.sum
    }
    val shared = share(parts)
    val decoded = FoldedLongReduce.reduce(shared, steps, width, threads, batchSize, plus)
    val expected = expectedChecksum(rows)
    var round = 0
    while (round < warmup) do
      val got = action()
      if (expected >= 0L && got != expected) then refuse(s"checksum $got != $expected")
      val _ = share(parts).length
      val _ = kernel(shared)
      val _ = output(decoded)
      round += 1
    val actionMs = new Array[Double](rounds)
    val insertMs = new Array[Double](rounds)
    val combineMs = new Array[Double](rounds)
    val exchangeMs = new Array[Double](rounds)
    val youngBefore = youngCollections()
    round = 0
    var checksum = 0L
    while (round < rounds) do
      val started = System.nanoTime()
      checksum = action()
      actionMs(round) = millis(System.nanoTime() - started)
      if (expected >= 0L && checksum != expected) then refuse(s"checksum $checksum != $expected")
      insertMs(round) = millis(FoldedLongReduce.insertNanos)
      combineMs(round) = millis(FoldedLongReduce.combineNanos)
      exchangeMs(round) = millis(FoldedLongReduce.exchangeNanos)
      round += 1
    val youngAfter = youngCollections()
    val shareMs = samples(rounds)(share(parts).length.toLong)
    val outputMs = samples(rounds)(output(decoded))
    val alloc = allocMedian(action)
    println(line("action", rows, median(actionMs), steady(actionMs)))
    println(line("insert", rows, median(insertMs), steady(insertMs)))
    println(line("combine", rows, median(combineMs), steady(combineMs)))
    println(line("exchange", rows, median(exchangeMs), steady(exchangeMs)))
    println(line("share", rows, median(shareMs), steady(shareMs)))
    println(line("output", rows, median(outputMs), steady(outputMs)))
    val youngTotal = youngCollections()
    val hits = DirectLongReduce.hitsForTest
    val peak = DirectLongReduce.peakSlotsForTest
    println(
      s"rows=$rows checksum=$checksum alloc_mb=${fmt(alloc)} young=${youngAfter - youngBefore} " +
        s"young_total=$youngTotal direct_hits=$hits direct_peak=$peak",
    )

  private def share(parts: Seq[com.ewoodbury.scarlet.core.Partition[_]]): Vector[ColumnBatch] =
    SourceEncode.shareOwnedLongPairs(parts).getOrElse(refuse("owned long pairs were not shared"))

  private def kernel(batches: Vector[ColumnBatch]): Int =
    val out = FoldedLongReduce.reduce(batches, steps, width, threads, batchSize, plus)
    out.foldLeft(0)((sum, batch) => sum + batch.length)

  private def output(batches: Vector[ColumnBatch]): Long =
    batches.foldLeft(0L) { (total, batch) =>
      BatchCodec.decodeLongPairs(batch).foldLeft(total) { (sum, row) =>
        sum + row._1 * 31L + row._2
      }
    }

  private def sourcePartitionsOf(
      plan: Plan[_],
  ): Seq[com.ewoodbury.scarlet.core.Partition[_]] =
    plan match
      case Plan.Source(parts, _) => parts
      case _ => refuse("filter-reducebykey source was not a scan")

  private def samples(rounds: Int)(body: => Long): Array[Double] =
    val taken = new Array[Double](rounds)
    var round = 0
    while (round < rounds) do
      val started = System.nanoTime()
      val produced = body
      taken(round) = millis(System.nanoTime() - started)
      if (produced < 0L) then refuse("phase produced a negative token")
      round += 1
    taken

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

  private def youngCollections(): Long =
    val beans = ManagementFactory.getGarbageCollectorMXBeans
    var index = 0
    var total = 0L
    while (index < beans.size) do
      val bean = beans.get(index)
      val name = bean.getName
      if (name.indexOf("Young") >= 0 || name.indexOf("young") >= 0) then
        val count = bean.getCollectionCount
        if (count > 0L) then total += count
      index += 1
    total

  private def expectedChecksum(rows: Int): Long =
    rows match
      case 1000000 => 166564205407L
      case 10000000 => 1663934572679L
      case _ => -1L

  private def lcgLongs(count: Int): Array[Long] =
    val generated = new Array[Long](count)
    var index = 0
    var state = seed
    while (index < count) do
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      generated(index) = ((mixed % 2000001) - 1000000).toLong
      index += 1
    generated

  /** Upper median, the same slot [[CrossEngineBench]] publishes as `p50_ms`. */
  private def median(samples: Array[Double]): Double =
    val copy = Arrays.copyOf(samples, samples.length)
    Arrays.sort(copy)
    copy(copy.length / 2)

  /** Upper median of the last 10 rounds. */
  private def steady(samples: Array[Double]): Double =
    val tail = math.min(10, samples.length)
    val copy = Arrays.copyOfRange(samples, samples.length - tail, samples.length)
    Arrays.sort(copy)
    copy(copy.length / 2)

  private def millis(nanos: Long): Double = nanos / 1_000_000.0

  private def line(phase: String, rows: Int, p50: Double, steadyMs: Double): String =
    s"phase=$phase rows=$rows p50_ms=${fmt(p50)} steady_ms=${fmt(steadyMs)}"

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
