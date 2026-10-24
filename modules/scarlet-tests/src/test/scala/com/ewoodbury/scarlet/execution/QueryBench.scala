package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.Locale

import scala.util.Random

import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Times one query per supported columnar type, plus both grouped-key patterns. Not a test, and
 * not part of `make test`.
 *
 * Each query is built once. The timed region is the action. The checksum is compared after the
 * timer stops. A local loop produces the expected value, and one row-path execution must match
 * it before any sample is kept. A mismatch fails the run.
 *
 * Widths 1 and 4 share one JVM. Each round runs both widths, and the width that runs first flips
 * every round. Columnar and row share that JVM too, and the layout that runs first flips with
 * the round. This is not a fresh-JVM isolation.
 *
 * `alloc_mb` is an estimate of bytes allocated on the calling thread during the action, measured
 * only at width 1, after the timed rounds. At that width the columnar kernels run on the caller.
 * The row scheduler can still run its task on another thread, so a row estimate can omit that
 * work. The figure is omitted at width 4. Do not compare it across thread counts.
 *
 * Nearest-rank percentiles use 1-based rank `ceil(p / 100 * n)` with no interpolation. With
 * `n = 21`, p50 is rank 11 and p95 is rank 20.
 *
 * {{{
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.QueryBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object QueryBench:

  private val warmup = 5
  private val measure = 21
  private val allocRounds = 5
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val elementRows = 1000000
  private val groupedRows = 10000000
  private val groups = 1024
  private val distinctStrings = 1000
  private val stringNeedle = 42
  private val lcgSeed = 0x12345678
  private val shuffledSeed = 58L
  private val threadCounts = Vector(1, 4)
  private val layouts = Vector(true, false)

  private enum Answer:
    case Whole(value: Long)
    case Real(value: Double)

  private final class Query(
      val name: String,
      val rows: Int,
      val shape: String,
      val expected: Answer,
      val run: () => Answer,
  ):
    val elapsed: Array[Array[Array[Long]]] = Array.ofDim[Long](2, 2, measure)
    val allocBytes: Array[Array[Long]] = Array.ofDim[Long](2, allocRounds)

    java.util.Arrays.fill(allocBytes(0), -1L)
    java.util.Arrays.fill(allocBytes(1), -1L)

  def main(args: Array[String]): Unit =
    require(rankIndex(measure, 50) == 10, "p50 rank drifted")
    require(rankIndex(measure, 95) == 19, "p95 rank drifted")
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val bean = enableCallerAlloc()
    try
      install(threads = 1, columnar = true)
      val queries = build()
      verifyRowOracle(queries)
      System.err.println(s"warm $warmup")
      rounds(queries, warmup, record = false)
      System.err.println(s"measure $measure")
      rounds(queries, measure, record = true)
      System.err.println(s"allocate $allocRounds")
      allocate(bean, queries)
      report(bean.isDefined, queries)
    finally
      ScarletConf.set(savedConf)
      ScarletRuntime.set(savedRuntime)

  private def build(): Vector[Query] =
    val generated = generate(elementRows)
    val narrowSum = expectNarrow(generated)
    val chainSum = expectPlus(generated, steps = 4)
    val doubleSum = expectPositiveDoubles(generated)
    val ints = DistCollection.ofInts(generated, sourcePartitions)
    val narrow = ints.filter(_ > 0).map(_ * 2)
    val intChain = ints.map(_ + 1).map(_ + 1).map(_ + 1).map(_ + 1)
    val longs = DistCollection.ofLongs(generated.map(_.toLong), sourcePartitions)
    val longChain = longs.map(_ + 1L).map(_ + 1L).map(_ + 1L).map(_ + 1L)
    val doubles = DistCollection.ofDoubles(generated.map(_.toDouble), sourcePartitions)
    val positive = doubles.filter(_ > 0.0)
    val texts = stringRows(elementRows)
    val strings = DistCollection.ofStrings(texts, sourcePartitions).filterStringEquals("k42")
    val flags = Array.tabulate(elementRows)(row => (row & 1) == 0)
    val bools = DistCollection.ofBools(flags, sourcePartitions).filter(identity)
    val (repeating, shuffled, groupedSum) = groupedSources()
    Vector(
      whole("int-narrow", elementRows, "Pipeline2(Scan)", narrowSum, narrow.plan, narrow.sum),
      whole("int-chain", elementRows, "Pipeline4(Scan)", chainSum, intChain.plan, intChain.sum),
      whole("long-chain", elementRows, "Pipeline4(Scan)", chainSum, longChain.plan, longChain.sum),
      real("double", elementRows, "Filter(Scan)", doubleSum, positive.plan, positive.sum),
      whole("string", elementRows, "Filter(Scan)", expectString(), strings.plan, strings.count()),
      whole("boolean", elementRows, "Filter(Scan)", expectBools(), bools.plan, bools.count()),
      whole(
        "grouped-repeat",
        groupedRows,
        "GroupedSum(Scan)",
        groupedSum,
        repeating.plan,
        repeating.groupedSum,
      ),
      whole(
        "grouped-shuffled",
        groupedRows,
        "GroupedSum(Scan)",
        groupedSum,
        shuffled.plan,
        shuffled.groupedSum,
      ),
    )

  private def whole(
      name: String,
      rows: Int,
      shape: String,
      expected: Long,
      plan: Plan[_],
      body: => Long,
  ): Query =
    requireShape(name, shape, plan)
    new Query(name, rows, shape, Answer.Whole(expected), () => Answer.Whole(body))

  private def real(
      name: String,
      rows: Int,
      shape: String,
      expected: Double,
      plan: Plan[_],
      body: => Double,
  ): Query =
    requireShape(name, shape, plan)
    new Query(name, rows, shape, Answer.Real(expected), () => Answer.Real(body))

  private def requireShape(name: String, shape: String, plan: Plan[_]): Unit =
    val got =
      if (shape.startsWith("GroupedSum")) then
        ColumnarPlanner.groupedSumPeel(plan).getOrElse("refused")
      else ColumnarPlanner.lower(plan).map(_.shape).getOrElse("refused")
    require(sameText(got, shape), s"$name shape $got != $shape")

  private def verifyRowOracle(queries: Vector[Query]): Unit =
    install(threads = 1, columnar = false)
    queries.foreach { query =>
      ScarletRuntime.get.shuffle.clear()
      val got = query.run()
      require(matches(got, query.expected), s"${query.name} row oracle ${show(got)}")
    }

  private def rounds(queries: Vector[Query], count: Int, record: Boolean): Unit =
    (0 until count).foreach { round =>
      val widths = if (round % 2 == 0) then threadCounts else threadCounts.reverse
      val order = if (round % 2 == 0) then layouts else layouts.reverse
      widths.foreach { threads =>
        order.foreach { columnar =>
          install(threads, columnar)
          queries.foreach(query => sample(query, threads, columnar, round, record))
        }
      }
      val phase = if (record) then "measure" else "warm"
      System.err.println(s"$phase ${round + 1}/$count")
    }

  private def sample(
      query: Query,
      threads: Int,
      columnar: Boolean,
      round: Int,
      record: Boolean,
  ): Unit =
    ScarletRuntime.get.shuffle.clear()
    val started = System.nanoTime()
    val got = query.run()
    val elapsed = System.nanoTime() - started
    val layout = layoutName(columnar)
    require(
      matches(got, query.expected),
      s"${query.name} $layout t$threads ${show(got)} != ${show(query.expected)}",
    )
    if (record) then query.elapsed(layoutIndex(columnar))(widthIndex(threads))(round) = elapsed

  private def allocate(bean: Option[ThreadMXBean], queries: Vector[Query]): Unit =
    bean.foreach { active =>
      val caller = Thread.currentThread().getId
      (0 until allocRounds).foreach { round =>
        val order = if (round % 2 == 0) then layouts else layouts.reverse
        order.foreach { columnar =>
          install(threads = 1, columnar = columnar)
          queries.foreach { query =>
            ScarletRuntime.get.shuffle.clear()
            val before = active.getThreadAllocatedBytes(caller)
            val got = query.run()
            val after = active.getThreadAllocatedBytes(caller)
            require(matches(got, query.expected), s"${query.name} alloc ${show(got)}")
            query.allocBytes(layoutIndex(columnar))(round) = after - before
          }
        }
        System.err.println(s"allocate ${round + 1}/$allocRounds")
      }
    }

  private def report(allocOn: Boolean, queries: Vector[Query]): Unit =
    println(preamble(allocOn))
    println()
    println("| query | layout | threads | rows | p50 ms | p95 ms | alloc mb |")
    println("| --- | --- | ---: | ---: | ---: | ---: | ---: |")
    queries.foreach { query =>
      layouts.foreach { columnar =>
        threadCounts.foreach { threads =>
          println(tableRow(query, columnar, threads, allocOn))
        }
      }
    }
    println()
    println(losses(queries))
    println()
    queries.foreach { query =>
      layouts.foreach { columnar =>
        threadCounts.foreach { threads =>
          println(jsonRow(query, columnar, threads, allocOn))
        }
      }
    }

  private def tableRow(query: Query, columnar: Boolean, threads: Int, allocOn: Boolean): String =
    val (p50, p95) = percentiles(query, columnar, threads)
    val alloc = allocText(query, columnar, threads, allocOn)
    s"| ${query.name} | ${layoutName(columnar)} | $threads | ${query.rows} | " +
      s"${millis(p50)} | ${millis(p95)} | $alloc |"

  private def jsonRow(query: Query, columnar: Boolean, threads: Int, allocOn: Boolean): String =
    val (p50, p95) = percentiles(query, columnar, threads)
    val alloc = allocJson(query, columnar, threads, allocOn)
    "BENCH {" +
      s""""query":"${query.name}",""" +
      s""""layout":"${layoutName(columnar)}",""" +
      s""""threads":$threads,""" +
      s""""p50_ms":${millis(p50)},""" +
      s""""p95_ms":${millis(p95)},""" +
      s""""alloc_mb":$alloc}"""

  private def losses(queries: Vector[Query]): String =
    val lines = queries.flatMap { query =>
      threadCounts.flatMap { threads =>
        val columnar = percentiles(query, columnar = true, threads)._1
        val row = percentiles(query, columnar = false, threads)._1
        val gap = columnar - row
        if (gap > 0L) then Some(s"- ${query.name} threads=$threads ${signedMillis(gap)} ms")
        else None
      }
    }
    if (lines.isEmpty) then "Columnar p50 is at or below the row p50 on every query and width."
    else "Columnar p50 above the row p50:\n" + lines.mkString("\n")

  private def percentiles(query: Query, columnar: Boolean, threads: Int): (Long, Long) =
    val samples = query.elapsed(layoutIndex(columnar))(widthIndex(threads)).clone()
    java.util.Arrays.sort(samples)
    (samples(rankIndex(samples.length, 50)), samples(rankIndex(samples.length, 95)))

  private def allocText(
      query: Query,
      columnar: Boolean,
      threads: Int,
      allocOn: Boolean,
  ): String =
    allocMedian(query, columnar, threads, allocOn) match
      case None => "n/a"
      case Some(bytes) => megabytes(bytes)

  private def allocJson(
      query: Query,
      columnar: Boolean,
      threads: Int,
      allocOn: Boolean,
  ): String =
    allocMedian(query, columnar, threads, allocOn) match
      case None => "null"
      case Some(bytes) => megabytes(bytes)

  /** Width 4 is omitted. A missing bean or a negative counter is omitted. */
  private def allocMedian(
      query: Query,
      columnar: Boolean,
      threads: Int,
      allocOn: Boolean,
  ): Option[Long] =
    if (!allocOn || threads != 1) then None
    else
      val samples = query.allocBytes(layoutIndex(columnar)).clone()
      java.util.Arrays.sort(samples)
      if (samples.exists(_ < 0L)) then None
      else Some(samples(rankIndex(samples.length, 50)))

  /** 1-based rank `ceil(percent / 100 * count)`, returned as a 0-based index. */
  private def rankIndex(count: Int, percent: Int): Int =
    val rank = (percent * count + 99) / 100
    math.max(1, math.min(count, rank)) - 1

  private def install(threads: Int, columnar: Boolean): Unit =
    val current = ScarletConf.get
    ScarletConf.set(
      current.copy(
        threadPoolSize = threads,
        columnarExecution = columnar,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = 4,
      ),
    )
    val runtime = ScarletRuntime.get
    ScarletRuntime.set(runtime.copy(scheduler = new LocalTaskScheduler(threads)))

  private def groupedSources()
      : (DistCollection[(Int, Int)], DistCollection[(Int, Int)], Long) =
    val keys = Array.tabulate(groupedRows)(row => row % groups)
    val values = Array.tabulate(groupedRows)(row => row)
    val shuffled = shuffleWithinMorsels(keys)
    val expected = groupedRows.toLong * (groupedRows.toLong - 1L) / 2L
    require(sumInts(values) == expected, "grouped values drifted from the closed sum")
    val repeating = DistCollection.ofIntPairs(keys, values, sourcePartitions)
    val permuted = DistCollection.ofIntPairs(shuffled, values, sourcePartitions)
    (repeating, permuted, expected)

  /**
   * Shuffles each morsel in place on a copy. Morsels are `columnarBatchSize` ranges from the
   * start of each source partition, which is how the columnar scan slices a partition. The key
   * multiset of each range stays equal to `row % groups`. Values stay in row order.
   */
  private def shuffleWithinMorsels(keys: Array[Int]): Array[Int] =
    val copy = keys.clone()
    val random = new Random(shuffledSeed)
    val partRows = keys.length / sourcePartitions
    require(partRows * sourcePartitions == keys.length, "grouped rows do not split evenly")
    var part = 0
    while (part < sourcePartitions) {
      val origin = part * partRows
      var start = 0
      while (start < partRows) {
        val length = math.min(batchSize, partRows - start)
        fisherYates(copy, origin + start, length, random)
        require(
          sameCounts(keys, copy, origin + start, length),
          s"morsel multiset changed at ${origin + start}",
        )
        start += length
      }
      part += 1
    }
    require(!keys.sameElements(copy), "shuffled keys kept sequential order")
    copy

  private def fisherYates(values: Array[Int], start: Int, length: Int, random: Random): Unit =
    var offset = length - 1
    while (offset > 0) {
      val swap = random.nextInt(offset + 1)
      val left = start + offset
      val right = start + swap
      val held = values(left)
      values(left) = values(right)
      values(right) = held
      offset -= 1
    }

  private def sameCounts(left: Array[Int], right: Array[Int], start: Int, length: Int): Boolean =
    val counts = new Array[Int](groups)
    var index = 0
    while (index < length) {
      counts(left(start + index)) += 1
      index += 1
    }
    index = 0
    var matched = true
    while (index < length && matched) {
      val key = right(start + index)
      counts(key) -= 1
      if (counts(key) < 0) then matched = false
      index += 1
    }
    var cursor = 0
    while (cursor < counts.length && matched) {
      if (counts(cursor) != 0) then matched = false
      cursor += 1
    }
    matched

  private def generate(rows: Int): Array[Int] =
    val values = new Array[Int](rows)
    var index = 0
    var state = lcgSeed
    while (index < rows) {
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      values(index) = (mixed % 2000001) - 1000000
      index += 1
    }
    values

  private def expectNarrow(values: Array[Int]): Long =
    var index = 0
    var sum = 0L
    while (index < values.length) {
      val value = values(index)
      if (value > 0) then sum += (value * 2).toLong
      index += 1
    }
    sum

  private def expectPlus(values: Array[Int], steps: Int): Long =
    var index = 0
    var sum = 0L
    val extra = steps.toLong
    while (index < values.length) {
      sum += values(index).toLong + extra
      index += 1
    }
    sum

  private def expectPositiveDoubles(values: Array[Int]): Double =
    var index = 0
    var sum = 0.0
    while (index < values.length) {
      val value = values(index).toDouble
      if (value > 0.0) then sum += value
      index += 1
    }
    sum

  private def expectString(): Long =
    var index = 0
    var count = 0L
    while (index < elementRows) {
      if (index % distinctStrings == stringNeedle) then count += 1L
      index += 1
    }
    count

  private def expectBools(): Long =
    var index = 0
    var count = 0L
    while (index < elementRows) {
      if ((index & 1) == 0) then count += 1L
      index += 1
    }
    count

  private def stringRows(rows: Int): Array[String] =
    val pool = Array.tabulate(distinctStrings)(index => "k" + index.toString)
    require(sameText(pool(stringNeedle), "k42"), "string needle drifted")
    Array.tabulate(rows)(row => pool(row % distinctStrings))

  private def sumInts(values: Array[Int]): Long =
    var index = 0
    var total = 0L
    while (index < values.length) {
      total += values(index).toLong
      index += 1
    }
    total

  private def preamble(allocOn: Boolean): String =
    val runtime = ManagementFactory.getRuntimeMXBean
    val alloc =
      if (allocOn) then "caller-thread-estimate"
      else "unavailable"
    s"query bench warmup=$warmup measure=$measure alloc_rounds=$allocRounds " +
      s"batch=$batchSize partitions=$sourcePartitions alloc=$alloc\n" +
      s"percentile=nearest-rank ceil(p/100*n) no-interpolation n=$measure " +
      s"p50_rank=${rankIndex(measure, 50) + 1} p95_rank=${rankIndex(measure, 95) + 1}\n" +
      "widths 1 and 4 share one JVM; the width that runs first flips each round. " +
      "Columnar and row share that JVM; the layout that runs first flips each round. " +
      "This is not a fresh-JVM isolation.\n" +
      "alloc_mb is the nearest-rank p50 of caller-thread bytes at width 1 only, " +
      "in decimal megabytes. It is an estimate. Width 4 omits it. " +
      "Do not compare it across thread counts.\n" +
      s"java=${runtime.getVmName} ${runtime.getVmVersion}"

  private def matches(got: Answer, expected: Answer): Boolean = (got, expected) match
    case (Answer.Whole(left), Answer.Whole(right)) => left == right
    case (Answer.Real(left), Answer.Real(right)) =>
      java.lang.Double.doubleToLongBits(left) == java.lang.Double.doubleToLongBits(right)
    case _ => false

  private def show(answer: Answer): String = answer match
    case Answer.Whole(value) => value.toString
    case Answer.Real(value) => String.format(Locale.ENGLISH, "%.6f", value)

  private def millis(nanos: Long): String =
    String.format(Locale.ENGLISH, "%.3f", nanos.toDouble / 1000000.0)

  private def signedMillis(nanos: Long): String =
    String.format(Locale.ENGLISH, "%+.3f", nanos.toDouble / 1000000.0)

  private def megabytes(bytes: Long): String =
    String.format(Locale.ENGLISH, "%.3f", bytes.toDouble / 1000000.0)

  private def sameText(left: String, right: String): Boolean =
    left.compareTo(right) == 0

  private def layoutName(columnar: Boolean): String =
    if (columnar) then "columnar" else "row"

  private def layoutIndex(columnar: Boolean): Int =
    if (columnar) then 0 else 1

  private def widthIndex(threads: Int): Int =
    if (threads == 1) then 0 else 1

  private def enableCallerAlloc(): Option[ThreadMXBean] =
    ManagementFactory.getThreadMXBean match
      case bean: ThreadMXBean if bean.isThreadAllocatedMemorySupported =>
        bean.setThreadAllocatedMemoryEnabled(true)
        if (bean.isThreadAllocatedMemoryEnabled) then Some(bean) else None
      case _ => None
