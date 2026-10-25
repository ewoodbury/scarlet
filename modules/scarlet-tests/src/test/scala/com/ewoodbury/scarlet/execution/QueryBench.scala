package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.Locale
import java.util.concurrent.locks.LockSupport

import scala.util.Random

import cats.effect.unsafe.implicits.global
import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{IntPairArrays, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Times one query per supported columnar type, plus both grouped-key patterns. Not a test, and
 * not part of `make test`.
 *
 * Each query is built once. The timed region is the action. The scalar checksum is compared after
 * the timer stops. A local loop produces that scalar, and one row-path execution must match it
 * before any sample is kept. A mismatch fails the run.
 *
 * Grouped queries also checksum per-group sums. The row-path oracle folds the pairs that path
 * reads. `groupedSum` itself returns only the grand total, which is unchanged if those pairs are
 * reassigned across keys. The columnar check folds the groups `ColumnPipeline.sumByKey` returns
 * and must match the oracle. Both checks run at every width, before warmup and again after the
 * measured rounds, outside the timer. `grouped-shuffled` uses the same fold. The two grouped
 * checksums must differ, because the shuffle reassigns keys and the grand totals stay equal.
 *
 * Every sample installs a [[CountingSink]] outside the timer. A columnar sample must publish at
 * least one batch. A row sample must publish none. The check runs at each width and each layout.
 * A mismatch fails the run.
 *
 * `runMain` executes in the sbt JVM. `Test / fork` is left false, so absolute times carry sbt and
 * JIT noise. Comparisons within one run are the signal.
 *
 * Width and layout order are counterbalanced on different bits of the round. The measure phase
 * starts one step later than warmup, so round 0 of measure is not round 0 of warmup. One
 * [[LocalTaskScheduler]] per width is built for the run and shut down at the end.
 *
 * `alloc_mb` is a process-wide estimate for both layouts and both widths: a probe sums
 * `getThreadAllocatedBytes` across live threads while the action runs, then the nearest-rank p50
 * of those samples is reported. The probe's own bytes are excluded. A worker that starts and
 * exits between reads is missing. Unrelated JVM threads are included. The same method is used for
 * row and columnar, so the column is comparable inside this run. It is still an estimate.
 *
 * Nearest-rank percentiles use 1-based rank `ceil(p / 100 * n)` with no interpolation. p95 has
 * samples above it; it is not a stable tail. `max` is the largest sample.
 *
 * {{{
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.QueryBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object QueryBench:

  private val schemaName = "query-bench-1"
  private val warmup = 5
  private val measure = 40
  private val measureOffset = 1
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

  /** Per-group sums folded in key order. `total` is the grand sum of those groups. */
  private final case class GroupFold(hash: Long, total: Long)

  private final class Query(
      val name: String,
      val rows: Int,
      val shape: String,
      val expected: Answer,
      val run: () => Answer,
      val groupHash: Option[Long],
      val rowGroups: () => GroupFold,
      val columnarGroups: () => GroupFold,
  ):
    val elapsed: Array[Array[Array[Long]]] = Array.ofDim[Long](2, 2, measure)
    val allocBytes: Array[Array[Array[Long]]] = Array.ofDim[Long](2, 2, allocRounds)

    (0 until 2).foreach { layout =>
      (0 until 2).foreach { width =>
        java.util.Arrays.fill(allocBytes(layout)(width), -1L)
      }
    }

  def main(args: Array[String]): Unit =
    checkHarness()
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val schedulers = threadCounts.map { threads =>
      threads -> new LocalTaskScheduler(threads)
    }.toMap
    try
      install(schedulers, threads = 1, columnar = true)
      val queries = build()
      verifyRowOracle(queries)
      checkGroups(queries)
      System.err.println(s"warm $warmup")
      rounds(queries, warmup, record = false, offset = 0)
      System.err.println(s"measure $measure")
      rounds(queries, measure, record = true, offset = measureOffset)
      checkGroups(queries)
      System.err.println(s"allocate $allocRounds")
      val allocOn = allocate(queries)
      report(allocOn, queries)
    finally
      try schedulers.values.foreach(_.shutdown().unsafeRunSync())
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
    val (repeating, shuffled, groupedTotal) = groupedSources()
    val repeatFold = sourceGroupFold(repeating)
    val shuffledFold = sourceGroupFold(shuffled)
    require(repeatFold.total == groupedTotal, "repeating groups drifted from the closed sum")
    require(shuffledFold.total == groupedTotal, "shuffled groups drifted from the closed sum")
    require(repeatFold.hash != shuffledFold.hash, "grouped checksums collided")
    Vector(
      whole("int-narrow", elementRows, "Pipeline2(Scan)", narrowSum, narrow.plan, narrow.sum),
      whole("int-chain", elementRows, "Pipeline4(Scan)", chainSum, intChain.plan, intChain.sum),
      whole("long-chain", elementRows, "Pipeline4(Scan)", chainSum, longChain.plan, longChain.sum),
      real("double", elementRows, "Filter(Scan)", doubleSum, positive.plan, positive.sum),
      whole("string", elementRows, "Filter(Scan)", expectString(), strings.plan, strings.count()),
      whole("boolean", elementRows, "Filter(Scan)", expectBools(), bools.plan, bools.count()),
      grouped("grouped-repeat", repeating, repeatFold.hash),
      grouped("grouped-shuffled", shuffled, shuffledFold.hash),
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
    new Query(
      name,
      rows,
      shape,
      Answer.Whole(expected),
      () => Answer.Whole(body),
      None,
      () => unusedGroups(name),
      () => unusedGroups(name),
    )

  private def real(
      name: String,
      rows: Int,
      shape: String,
      expected: Double,
      plan: Plan[_],
      body: => Double,
  ): Query =
    requireShape(name, shape, plan)
    new Query(
      name,
      rows,
      shape,
      Answer.Real(expected),
      () => Answer.Real(body),
      None,
      () => unusedGroups(name),
      () => unusedGroups(name),
    )

  private def grouped(
      name: String,
      source: DistCollection[(Int, Int)],
      hash: Long,
  ): Query =
    requireShape(name, "GroupedSum(Scan)", source.plan)
    new Query(
      name,
      groupedRows,
      "GroupedSum(Scan)",
      Answer.Whole(sourceGroupFold(source).total),
      () => Answer.Whole(source.groupedSum),
      Some(hash),
      () => sourceGroupFold(source),
      () => columnarGroupFold(source),
    )

  private def unusedGroups(name: String): GroupFold =
    throw new IllegalStateException(s"$name has no group checksum")

  private def requireShape(name: String, shape: String, plan: Plan[_]): Unit =
    val got =
      if (shape.startsWith("GroupedSum")) then
        ColumnarPlanner.groupedSumPeel(plan).getOrElse("refused")
      else ColumnarPlanner.lower(plan).map(_.shape).getOrElse("refused")
    require(sameText(got, shape), s"$name shape $got != $shape")

  private def verifyRowOracle(queries: Vector[Query]): Unit =
    install(threads = 1, columnar = false)
    queries.foreach { query =>
      val got = runObserved(query, threads = 1, columnar = false)
      require(matches(got, query.expected), s"${query.name} row oracle ${show(got)}")
    }

  /**
   * Row folds must reproduce the stored checksum. Columnar folds `sumByKey` at this width and
   * must reproduce it too. The public action's grand total is checked on both layouts.
   */
  private def checkGroups(queries: Vector[Query]): Unit =
    threadCounts.foreach { threads =>
      queries.foreach { query =>
        query.groupHash.foreach { expected =>
          install(threads, columnar = false)
          val rowFold = query.rowGroups()
          require(
            rowFold.hash == expected,
            s"${query.name} row t$threads group checksum ${rowFold.hash} != $expected",
          )
          val rowTotal = runObserved(query, threads, columnar = false)
          require(matches(rowTotal, query.expected), s"${query.name} row t$threads total")
          install(threads, columnar = true)
          val columnarFold = observedGroups(query, threads)
          require(
            columnarFold.hash == expected,
            s"${query.name} columnar t$threads group checksum ${columnarFold.hash} != $expected",
          )
          require(
            columnarFold.total == rowFold.total,
            s"${query.name} columnar t$threads group total ${columnarFold.total}",
          )
          val columnarTotal = runObserved(query, threads, columnar = true)
          require(
            matches(columnarTotal, query.expected),
            s"${query.name} columnar t$threads total",
          )
        }
      }
    }

  private def observedGroups(query: Query, threads: Int): GroupFold =
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    try
      val fold = query.columnarGroups()
      require(
        sink.batchCount > 0L,
        s"${query.name} columnar t$threads group check published no column batch",
      )
      fold
    finally ColumnMetrics.restore(previous)

  private def rounds(queries: Vector[Query], count: Int, record: Boolean, offset: Int): Unit =
    var round = 0
    while (round < count) {
      val phase = round + offset
      val widthOrder = widthsFor(phase)
      val layoutOrder = layoutsFor(phase)
      widthOrder.foreach { threads =>
        layoutOrder.foreach { columnar =>
          install(threads, columnar)
          queries.foreach(query => sample(query, threads, columnar, round, record))
        }
      }
      val phaseName = if (record) then "measure" else "warm"
      System.err.println(s"$phaseName ${round + 1}/$count")
      round += 1
    }

  private def sample(
      query: Query,
      threads: Int,
      columnar: Boolean,
      round: Int,
      record: Boolean,
  ): Unit =
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    try
      ScarletRuntime.get.shuffle.clear()
      val started = System.nanoTime()
      val got = query.run()
      val elapsed = System.nanoTime() - started
      requirePath(query.name, columnar, threads, sink.batchCount)
      require(
        matches(got, query.expected),
        s"${query.name} ${layoutName(columnar)} t$threads ${show(got)} != ${show(query.expected)}",
      )
      if (record) then
        query.elapsed(layoutIndex(columnar))(widthIndex(threads))(round) = elapsed
    finally ColumnMetrics.restore(previous)

  private def runObserved(query: Query, threads: Int, columnar: Boolean): Answer =
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    try
      ScarletRuntime.get.shuffle.clear()
      val got = query.run()
      requirePath(query.name, columnar, threads, sink.batchCount)
      got
    finally ColumnMetrics.restore(previous)

  private def requirePath(name: String, columnar: Boolean, threads: Int, batches: Long): Unit =
    val layout = layoutName(columnar)
    if (columnar) then
      require(batches > 0L, s"$name $layout t$threads published no column batch")
    else require(batches == 0L, s"$name $layout t$threads published $batches column batches")

  /**
   * Process-wide bytes for every width and layout. The probe runs only in this pass, so it does
   * not sit on the timed samples.
   */
  private def allocate(queries: Vector[Query]): Boolean =
    startProbe() match
      case None =>
        System.err.println("allocate unavailable")
        false
      case Some(probe) =>
        try
          var round = 0
          while (round < allocRounds) {
            val phase = round + measureOffset
            widthsFor(phase).foreach { threads =>
              layoutsFor(phase).foreach { columnar =>
                install(threads, columnar)
                queries.foreach { query =>
                  val sink = new CountingSink
                  val previous = ColumnMetrics.install(sink)
                  try
                    val (got, bytes) = probe.account {
                      ScarletRuntime.get.shuffle.clear()
                      query.run()
                    }
                    requirePath(query.name, columnar, threads, sink.batchCount)
                    require(
                      matches(got, query.expected),
                      s"${query.name} alloc ${show(got)}",
                    )
                    query.allocBytes(layoutIndex(columnar))(widthIndex(threads))(round) = bytes
                  finally ColumnMetrics.restore(previous)
                }
              }
            }
            System.err.println(s"allocate ${round + 1}/$allocRounds")
            round += 1
          }
          true
        finally probe.stop()

  private def report(allocOn: Boolean, queries: Vector[Query]): Unit =
    val header = Vector(
      preamble(allocOn),
      "",
      "| query | layout | threads | rows | p50 ms | p95 ms | max ms | alloc mb |",
      "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    )
    val body = queries.flatMap { query =>
      layouts.flatMap { columnar =>
        threadCounts.map(threads => tableRow(query, columnar, threads, allocOn))
      }
    }
    System.err.println((header ++ body ++ Vector("", losses(queries), "")).mkString("\n"))
    queries.foreach { query =>
      layouts.foreach { columnar =>
        threadCounts.foreach { threads =>
          println(jsonRow(query, columnar, threads, allocOn))
        }
      }
    }

  private def tableRow(query: Query, columnar: Boolean, threads: Int, allocOn: Boolean): String =
    val (median, tail, highest) = percentiles(query, columnar, threads)
    val alloc = allocText(query, columnar, threads, allocOn)
    s"| ${query.name} | ${layoutName(columnar)} | $threads | ${query.rows} | " +
      s"${millis(median)} | ${millis(tail)} | ${millis(highest)} | $alloc |"

  private def jsonRow(query: Query, columnar: Boolean, threads: Int, allocOn: Boolean): String =
    val (median, tail, highest) = percentiles(query, columnar, threads)
    val alloc = allocJson(query, columnar, threads, allocOn)
    val checksum = query.groupHash.getOrElse(checksumOf(query.expected))
    "{" +
      s""""schema":"$schemaName",""" +
      s""""query":"${query.name}",""" +
      s""""layout":"${layoutName(columnar)}",""" +
      s""""threads":$threads,""" +
      s""""rows":${query.rows},""" +
      s""""batch":$batchSize,""" +
      s""""rounds":$measure,""" +
      s""""p50_ms":${millis(median)},""" +
      s""""p95_ms":${millis(tail)},""" +
      s""""max_ms":${millis(highest)},""" +
      s""""alloc_mb":$alloc,""" +
      s""""checksum":$checksum}"""

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

  private def percentiles(query: Query, columnar: Boolean, threads: Int): (Long, Long, Long) =
    val samples = query.elapsed(layoutIndex(columnar))(widthIndex(threads)).clone()
    java.util.Arrays.sort(samples)
    val count = samples.length
    (samples(rankIndex(count, 50)), samples(rankIndex(count, 95)), samples(count - 1))

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

  private def allocMedian(
      query: Query,
      columnar: Boolean,
      threads: Int,
      allocOn: Boolean,
  ): Option[Long] =
    if (!allocOn) then None
    else
      val samples = query.allocBytes(layoutIndex(columnar))(widthIndex(threads)).clone()
      java.util.Arrays.sort(samples)
      if (samples.exists(_ < 0L)) then None
      else Some(samples(rankIndex(samples.length, 50)))

  /** 1-based rank `ceil(percent / 100 * count)`, returned as a 0-based index. */
  private def rankIndex(count: Int, percent: Int): Int =
    val rank = (percent * count + 99) / 100
    math.max(1, math.min(count, rank)) - 1

  private def checkHarness(): Unit =
    require(measure % 4 == 0, s"measure=$measure does not split width and layout evenly")
    require(warmup > 0 && allocRounds > 0, "warmup and allocation rounds must be positive")
    val p50 = rankIndex(measure, 50)
    val p95 = rankIndex(measure, 95)
    require(p50 < p95, s"p50 index $p50 is not below p95 index $p95")
    require(p95 < measure - 2, s"p95 index $p95 is in the top two samples of $measure")
    val columnarLeads = Array(0, 0)
    val widthLeads = Array(0, 0)
    var round = 0
    while (round < measure) {
      val phase = round + measureOffset
      columnarLeads(layoutIndex(leader(layoutsFor(phase)))) += 1
      widthLeads(widthIndex(leader(widthsFor(phase)))) += 1
      round += 1
    }
    require(columnarLeads(0) == columnarLeads(1), "layout leaders are unbalanced")
    require(widthLeads(0) == widthLeads(1), "width leaders are unbalanced")
    val layoutRestarts = leader(layoutsFor(0)) == leader(layoutsFor(measureOffset))
    val widthRestarts = leader(widthsFor(0)) == leader(widthsFor(measureOffset))
    require(!(layoutRestarts && widthRestarts), "measure restarts in the warmup order")

  private def leader[A](order: Vector[A]): A =
    order match
      case head +: _ => head
      case _ => throw new IllegalStateException("order is empty")

  /** Width order follows bit 0. Layout order follows bit 1, so the two are not tied together. */
  private def widthsFor(phase: Int): Vector[Int] =
    if (phase % 2 == 0) then threadCounts else threadCounts.reverse

  private def layoutsFor(phase: Int): Vector[Boolean] =
    if ((phase / 2) % 2 == 0) then layouts else layouts.reverse

  private def install(threads: Int, columnar: Boolean): Unit =
    install(liveSchedulers, threads, columnar)

  private var liveSchedulers: Map[Int, LocalTaskScheduler] = Map.empty

  private def install(
      schedulers: Map[Int, LocalTaskScheduler],
      threads: Int,
      columnar: Boolean,
  ): Unit =
    liveSchedulers = schedulers
    val scheduler = schedulers.getOrElse(
      threads,
      throw new IllegalStateException(s"no scheduler for width $threads"),
    )
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
    ScarletRuntime.set(runtime.copy(scheduler = scheduler))

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
   * multiset of each range stays equal to `row % groups`. Values stay in row order, so the grand
   * total is unchanged and the per-group sums are not.
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

  /**
   * Folds pairs the row path reads. Present keys contribute `(key * 31) ^ groupSum`. The sum of
   * those terms is the checksum. Key order makes it independent of row order.
   */
  private def sourceGroupFold(source: DistCollection[(Int, Int)]): GroupFold =
    val sums = new Array[Long](groups)
    val seen = new Array[Int](groups)
    source.plan match
      case Plan.Source(partitions, _) =>
        partitions.foreach { partition =>
          partition.data match
            case pairs: IntPairArrays => addPairs(pairs.keys, pairs.values, sums, seen)
            case _ =>
              throw new IllegalStateException("grouped source was not an owned pair array")
        }
      case _ => throw new IllegalStateException("grouped plan was not a source")
    finishGroups(sums, seen)

  /**
   * Folds the groups the columnar kernel returns. Partials are already exchanged, and a key that
   * appears in more than one batch is accumulated before the fold.
   */
  private def columnarGroupFold(source: DistCollection[(Int, Int)]): GroupFold =
    require(
      ScarletConf.get.columnarExecution,
      "columnar group checksum ran with columnar execution off",
    )
    source.plan match
      case sourcePlan: Plan.Source[_] =>
        val encoded = ColumnarInterpreter.encodeGroupedPairs(sourcePlan.partitions)
        val summed = ColumnPipeline.sumByKey(
          encoded,
          keyOrdinal = 0,
          valueOrdinal = 1,
          numPartitions = ColumnarPlanner.shuffleWidth,
          parallelism = ColumnarPlanner.parallelism,
          batchSize = ColumnarPlanner.batchSize,
          steps = Vector.empty[PairStep],
        )
        val sums = new Array[Long](groups)
        val seen = new Array[Int](groups)
        summed.foreach(batch => addBatch(batch, sums, seen))
        finishGroups(sums, seen)
      case _ => throw new IllegalStateException("grouped plan was not a source")

  private def addPairs(
      keys: Array[Int],
      values: Array[Int],
      sums: Array[Long],
      seen: Array[Int],
  ): Unit =
    require(keys.length == values.length, "grouped key and value lengths differ")
    var index = 0
    while (index < keys.length) {
      addGroup(keys(index), values(index).toLong, sums, seen)
      index += 1
    }

  private def addBatch(batch: ColumnBatch, sums: Array[Long], seen: Array[Int]): Unit =
    if (batch.length != 0) then
      val keys = intKeys(batch)
      val values = longSums(batch)
      var row = 0
      while (row < batch.length) {
        require(keys.isValid(row) && values.isValid(row), "grouped checksum saw a null cell")
        addGroup(keys.values(row), values.values(row), sums, seen)
        row += 1
      }

  private def addGroup(key: Int, value: Long, sums: Array[Long], seen: Array[Int]): Unit =
    require(key >= 0 && key < groups, s"grouped checksum key $key outside 0 until $groups")
    sums(key) += value
    seen(key) += 1

  private def finishGroups(sums: Array[Long], seen: Array[Int]): GroupFold =
    var key = 0
    var hash = 0L
    var total = 0L
    var present = 0
    while (key < groups) {
      if (seen(key) > 0) then
        present += 1
        total += sums(key)
        hash += (key.toLong * 31L) ^ sums(key)
      key += 1
    }
    require(present == groups, s"grouped checksum saw $present groups, expected $groups")
    GroupFold(hash, total)

  private def intKeys(batch: ColumnBatch): Int32Column =
    batch.columns.headOption match
      case Some(column: Int32Column) => column
      case _ => throw new IllegalStateException("grouped key column was not int32")

  private def longSums(batch: ColumnBatch): Int64Column =
    batch.columns.lift(1) match
      case Some(column: Int64Column) => column
      case _ => throw new IllegalStateException("grouped sum column was not int64")

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
    val alloc = if (allocOn) then "process-wide-estimate" else "unavailable"
    val p50Rank = rankIndex(measure, 50) + 1
    val p95Rank = rankIndex(measure, 95) + 1
    s"query bench warmup=$warmup measure=$measure alloc_rounds=$allocRounds " +
      s"batch=$batchSize partitions=$sourcePartitions alloc=$alloc\n" +
      s"percentile=nearest-rank ceil(p/100*n) no-interpolation n=$measure " +
      s"p50_rank=$p50Rank p95_rank=$p95Rank max_rank=$measure\n" +
      s"p95 is rank $p95Rank of $measure, so ${measure - p95Rank} sample(s) sit strictly above it. " +
      "It is not a stable tail. max is the largest sample.\n" +
      "runMain executes in the sbt JVM with Test / fork left false. Absolute numbers carry " +
      "sbt and JIT noise. Comparisons within this run are the signal.\n" +
      "Widths 1 and 4 share one JVM. Width order and layout order flip on different bits of " +
      "the round, and each leads on half of the measured rounds. Measure round 0 uses the " +
      "order of warmup round 1. This is not a fresh-JVM isolation.\n" +
      "Each sample fails the run unless columnar published at least one column batch and row " +
      "published none.\n" +
      "Grouped checksums fold (key * 31) xor the per-group sum, in key order. The row oracle " +
      "reads the pairs. The columnar check reads sumByKey. Both run before warmup and after " +
      "the measured rounds, at each width.\n" +
      "alloc_mb is the nearest-rank p50 of process-wide thread bytes, in decimal megabytes " +
      "(bytes / 1e6). The probe excludes itself. A short-lived worker missed between reads is " +
      "absent. The same estimate is reported for row and columnar, at both widths.\n" +
      s"java=${runtime.getVmName} ${runtime.getVmVersion}"

  private def checksumOf(answer: Answer): Long = answer match
    case Answer.Whole(value) => value
    case Answer.Real(value) => java.lang.Double.doubleToLongBits(value)

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

  private def startProbe(): Option[AllocProbe] =
    threadBean.filter(_.isThreadAllocatedMemorySupported).flatMap { bean =>
      bean.setThreadAllocatedMemoryEnabled(true)
      if (bean.isThreadAllocatedMemoryEnabled) then Some(new AllocProbe(bean)) else None
    }

  private def threadBean: Option[ThreadMXBean] =
    ManagementFactory.getThreadMXBean match
      case bean: ThreadMXBean => Some(bean)
      case _ => None

  /**
   * Sums bytes on every live thread except the probe. Morsel workers have exited when the action
   * returns, so a before/after pair on the caller misses them. The watch reads while the action
   * runs. A worker that is never seen is missing. The caller's before/after pair is included
   * because the caller is one of the live threads.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class AllocProbe(bean: ThreadMXBean):
    private val capacity = 1024
    private val ids = new Array[Long](capacity)
    private val baseline = new Array[Long](capacity)
    private val high = new Array[Long](capacity)
    private var count = 0
    @volatile private var running = true
    @volatile private var probeId = 0L
    private val thread = new Thread(() => watch(), "scarlet-alloc-probe")

    thread.setDaemon(true)
    thread.start()
    while (probeId == 0L) LockSupport.parkNanos(10000L)

    def stop(): Unit =
      running = false
      LockSupport.unpark(thread)
      thread.join(1000L)
      ()

    def account[A](body: => A): (A, Long) =
      open()
      try
        val value = body
        (value, close())
      catch
        case thrown: Throwable =>
          closeQuietly()
          throw thrown

    private def watch(): Unit =
      probeId = Thread.currentThread().getId
      while (running) {
        observe(setBaseline = false)
        LockSupport.parkNanos(50000L)
      }

    private def open(): Unit = this.synchronized {
      count = 0
      observe(setBaseline = true)
    }

    private def close(): Long = this.synchronized {
      observe(setBaseline = false)
      var index = 0
      var total = 0L
      while (index < count) {
        if (ids(index) != probeId) then
          val base = if (baseline(index) < 0L) 0L else baseline(index)
          val delta = high(index) - base
          if (delta > 0L) then total += delta
        index += 1
      }
      total
    }

    private def closeQuietly(): Unit =
      try
        val _ = close()
      catch
        case _: IllegalStateException => ()

    private def observe(setBaseline: Boolean): Unit = this.synchronized {
      val live = bean.getAllThreadIds
      var index = 0
      while (index < live.length) {
        val id = live(index)
        val bytes = bean.getThreadAllocatedBytes(id)
        if (bytes >= 0L) then note(id, bytes, setBaseline)
        index += 1
      }
    }

    private def note(id: Long, bytes: Long, setBaseline: Boolean): Unit =
      val at = slot(id)
      if (bytes > high(at)) then high(at) = bytes
      if (setBaseline) then baseline(at) = bytes

    private def slot(id: Long): Int =
      val existing = indexOf(id)
      if (existing >= 0) then existing
      else if (count >= capacity) then
        throw new IllegalStateException(s"alloc probe filled $capacity thread slots")
      else
        val placed = count
        ids(placed) = id
        baseline(placed) = -1L
        high(placed) = 0L
        count = placed + 1
        placed

    private def indexOf(id: Long): Int =
      var index = 0
      var found = -1
      while (index < count && found < 0) {
        if (ids(index) == id) then found = index
        index += 1
      }
      found
