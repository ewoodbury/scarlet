package com.ewoodbury.scarlet.execution
import java.nio.file.{Files, Path}
import java.util.Locale

import scala.util.control.NonFatal

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler
import com.ewoodbury.scarlet.support.Check

/**
 * End-to-end spill at the cross-engine row count. Not a test, and not part of `make test`.
 *
 * `filter-sort`, `filter-reducebykey`, and `filter-join` match [[CrossEngineBench]]: two filters,
 * two source partitions, batch 8192, shuffle width 4. `filter-reducebykey` is the long-pair
 * action. `filter-join` counts that join and does not build a table. `filter-joinsum` writes the
 * match columns, so the int shuffle-hash build runs. The filtered left side is the build.
 * `long-sort-merge` and `string-sort-merge` are explicit sort-merge joins of the same two sides.
 * Those kernels throw when spill is on and the present-key estimate is over the budget.
 *
 * Spill files are deleted before the action returns. `files` is the publish count of the last
 * measured round, not bytes left on disk. `passes` is [[RankSplitRuns]] for the sort and zero for
 * the other queries.
 *
 * Run `scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.SpillScaleBench` in a forked
 * test JVM. Assign `Test / javaOptions` with `:=`. A previous `-Xmx` stays on the sbt session when
 * the options are appended. The measured commands are in `docs/BENCHMARKS.md`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Any"))
object SpillScaleBench:

  private val groups = 1024
  private val seed = 0x12345678
  private val shuffleSeed = 58L
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val shufflePartitions = 4
  private val defaultBudget = 64L * 1024L * 1024L

  private val knownAt10m = Map(
    "filter-sort" -> 518322534674927L,
    "filter-reducebykey" -> 1663934572679L,
    "filter-join" -> 33281390L,
  )

  private final case class Settings(
      query: String,
      rows: Int,
      spill: Boolean,
      budget: Long,
      threads: Int,
      warmup: Int,
      rounds: Int,
  )

  private final case class Counts(
      sortFiles: Long,
      sortPasses: Long,
      reduceFiles: Long,
      joinFiles: Long,
      mergeFiles: Long,
  )

  private final case class Reading(
      query: String,
      status: String,
      p50Ms: Double,
      checksum: Long,
      counts: Counts,
      message: String,
  )

  def main(args: Array[String]): Unit =
    val settings = readSettings()
    val names = settings.query match
      case "suite" => Vector("filter-sort", "filter-reducebykey", "filter-join")
      case "throws" => Vector("long-sort-merge", "string-sort-merge")
      case name => Vector(name)
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(settings.threads)
    val directory = Files.createTempDirectory("spill-scale-bench")
    var status = 0
    try
      install(savedConf, savedRuntime, scheduler, directory, settings)
      System.err.println(
        s"spill-scale rows=${settings.rows} spill=${settings.spill} budget=${settings.budget} " +
          s"threads=${settings.threads} warmup=${settings.warmup} rounds=${settings.rounds} " +
          s"max_heap=${Runtime.getRuntime.maxMemory} dir=$directory",
      )
      names.foreach { name =>
        val reading = runQuery(name, settings)
        println(render(settings, reading))
        if (!Check.same(reading.status, "ok") && !Check.same(reading.status, "throw")) then
          status = 1
        else if (!checksumOk(name, settings.rows, reading)) then status = 1
      }
    catch
      case NonFatal(thrown) =>
        thrown.printStackTrace(System.err)
        status = 1
    finally
      try scheduler.shutdown()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)
        deleteDirectory(directory)
    if (status != 0) then sys.exit(status)

  private def runQuery(name: String, settings: Settings): Reading =
    System.err.println(s"QUERY_BEGIN $name ${System.currentTimeMillis()}")
    val built = build(name, settings.rows)
    val shape = ColumnarPlanner.lower(built.plan).map(_.shape).getOrElse("refused")
    System.err.println(s"ARM $name $shape")
    if (Check.same(shape, "refused")) then
      Reading(name, "refused", Double.NaN, 0L, readCounts(), "planner refused the plan")
    else if (throwsByDesign(name)) then runOnce(name, built.action)
    else timeMany(name, built.action, settings.warmup, settings.rounds)

  private def throwsByDesign(name: String): Boolean =
    Check.same(name, "long-sort-merge") || Check.same(name, "string-sort-merge")

  private final case class Built(action: () => Long, plan: Plan[_])

  private def build(name: String, rows: Int): Built = name match
    case "filter-sort" =>
      val order = permutation(rows)
      val values = lcgValues(rows)
      val sorted = filterInts(
        DistCollection.ofIntPairs(order, values, sourcePartitions),
      ).sortByIntKey
      Built(() => SortChecksum.of(sorted), sorted.plan)
    case "filter-reducebykey" =>
      val keys = Array.tabulate(rows)(row => (row % groups).toLong)
      val values = lcgLongs(rows)
      val reduced = filterLongs(DistCollection.ofLongPairs(keys, values, sourcePartitions))
        .reduceByKey[Long, Long](_ + _)
      Built(() => reduceChecksum(reduced.collect()), reduced.plan)
    case "filter-join" =>
      val keys = Array.tabulate(rows)(row => row % groups)
      val values = lcgValues(rows)
      val width = 10 * groups
      val joinKeys = Array.tabulate(width)(row => row % groups)
      val joinValues = Array.tabulate(width)(row => row % 100)
      val side = DistCollection.ofIntPairs(joinKeys, joinValues, sourcePartitions)
      val joined = filterInts(DistCollection.ofIntPairs(keys, values, sourcePartitions)).join(side)
      Built(() => joined.count(), joined.plan)
    case "filter-joinsum" =>
      val keys = Array.tabulate(rows)(row => row % groups)
      val values = lcgValues(rows)
      val width = 10 * groups
      val joinKeys = Array.tabulate(width)(row => row % groups)
      val joinValues = Array.tabulate(width)(row => row % 100)
      val side = DistCollection.ofIntPairs(joinKeys, joinValues, sourcePartitions)
      val joined = filterInts(DistCollection.ofIntPairs(keys, values, sourcePartitions)).join(side)
      Built(() => JoinValueSum.of(joined), joined.plan)
    case "long-sort-merge" =>
      val keys = Array.tabulate(rows)(row => (row % groups).toLong)
      val values = lcgLongs(rows)
      val left = filterLongs(DistCollection.ofLongPairs(keys, values, sourcePartitions))
      val right = filterLongs(DistCollection.ofLongPairs(keys, values, sourcePartitions))
      val joined = left.sortMergeJoin(right)
      Built(() => joined.count(), joined.plan)
    case "string-sort-merge" =>
      val dictionary = Array.tabulate(groups)(index => index.toString)
      val values = lcgValues(rows)
      val pairs = Vector.tabulate(rows)(index => dictionary(index % groups) -> values(index))
      val left = DistCollection(pairs, sourcePartitions)
      val right = DistCollection(pairs, sourcePartitions)
      val joined = left.sortMergeJoin(right)
      Built(() => joined.count(), joined.plan)
    case other =>
      throw new IllegalArgumentException(
        "SPILL_SCALE_QUERY must be suite, throws, filter-sort, filter-reducebykey, " +
          s"filter-join, filter-joinsum, long-sort-merge, or string-sort-merge, found $other",
      )

  private def timeMany(name: String, action: () => Long, warmup: Int, rounds: Int): Reading =
    var round = 0
    while (round < warmup) do
      ScarletRuntime.get.shuffle.clear()
      resetCounters()
      val _ = action()
      round += 1
    val samples = new Array[Double](rounds)
    var checksum = 0L
    var counts = readCounts()
    round = 0
    while (round < rounds) do
      ScarletRuntime.get.shuffle.clear()
      resetCounters()
      val started = System.nanoTime()
      checksum = action()
      samples(round) = (System.nanoTime() - started) / 1.0e6
      counts = readCounts()
      round += 1
    Reading(name, "ok", median(samples), checksum, counts, "")

  private def runOnce(name: String, action: () => Long): Reading =
    ScarletRuntime.get.shuffle.clear()
    resetCounters()
    val started = System.nanoTime()
    try
      val checksum = action()
      val elapsed = (System.nanoTime() - started) / 1.0e6
      Reading(name, "ok", elapsed, checksum, readCounts(), "")
    catch
      case NonFatal(thrown) =>
        val elapsed = (System.nanoTime() - started) / 1.0e6
        val text = Option(thrown.getMessage).getOrElse("")
        Reading(name, "throw", elapsed, 0L, readCounts(), s"${thrown.getClass.getName}: $text")

  private def checksumOk(name: String, rows: Int, reading: Reading): Boolean =
    if (!Check.same(reading.status, "ok") || rows != 10000000) then true
    else
      knownAt10m.get(name) match
        case Some(expected) if reading.checksum != expected =>
          System.err.println(s"checksum ${reading.checksum} != $expected for $name")
          false
        case _ => true

  private def filterInts(data: DistCollection[(Int, Int)]): DistCollection[(Int, Int)] =
    data.filterKeys((key: Int) => key % 3 != 0).filterValues((value: Int) => value >= 0)

  private def filterLongs(data: DistCollection[(Long, Long)]): DistCollection[(Long, Long)] =
    data.filterKeys((key: Long) => key % 3L != 0L).filterValues((value: Long) => value >= 0L)

  private def reduceChecksum(rows: Iterable[(Long, Long)]): Long =
    var total = 0L
    val iterator = rows.iterator
    while (iterator.hasNext) do
      val row = iterator.next()
      total += row._1 * 31L + row._2
    total

  private def resetCounters(): Unit =
    RankSplitRuns.resetForTest()
    LongHashReduceSpill.resetForTest()
    HashReduceSpill.resetForTest()
    HashJoinSpill.resetForTest()
    SortMergeRuns.resetForTest()

  private def readCounts(): Counts =
    Counts(
      RankSplitRuns.filesWrittenForTest,
      RankSplitRuns.mergePassesForTest,
      LongHashReduceSpill.filesWrittenForTest + HashReduceSpill.filesWrittenForTest,
      HashJoinSpill.filesWrittenForTest,
      SortMergeRuns.filesWrittenForTest,
    )

  private def install(
      savedConf: ScarletConf,
      savedRuntime: ScarletRuntime.RuntimeComponents,
      scheduler: LocalTaskScheduler,
      directory: Path,
      settings: Settings,
  ): Unit =
    ScarletConf.set(
      savedConf.copy(
        threadPoolSize = settings.threads,
        columnarExecution = true,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = shufflePartitions,
        spillEnabled = settings.spill,
        spillBudgetBytes = settings.budget,
        spillDirectory = directory.toString,
      ),
    )
    ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))

  private def render(settings: Settings, reading: Reading): String =
    val message = reading.message.replace('\n', ' ')
    val fields = List(
      s"query=${reading.query}",
      s"status=${reading.status}",
      s"spill=${settings.spill}",
      s"budget=${settings.budget}",
      s"rows=${settings.rows}",
      s"threads=${settings.threads}",
      s"p50_ms=${fmt(reading.p50Ms)}",
      s"checksum=${reading.checksum}",
      s"sort_files=${reading.counts.sortFiles}",
      s"sort_passes=${reading.counts.sortPasses}",
      s"reduce_files=${reading.counts.reduceFiles}",
      s"join_files=${reading.counts.joinFiles}",
      s"merge_files=${reading.counts.mergeFiles}",
      s"message=$message",
    )
    "SCALE " + fields.mkString(" ")

  private def fmt(millis: Double): String =
    if (millis.isNaN) then "nan" else String.format(Locale.ENGLISH, "%.3f", millis)

  private def median(samples: Array[Double]): Double =
    val copy = samples.clone()
    java.util.Arrays.sort(copy)
    copy(copy.length / 2)

  private def lcgValues(count: Int): Array[Int] =
    val values = new Array[Int](count)
    var index = 0
    var state = seed
    while (index < count) do
      state = state * 1664525 + 1013904223
      values(index) = ((state & 0x7fffffff) % 2000001) - 1000000
      index += 1
    values

  private def lcgLongs(count: Int): Array[Long] =
    val values = lcgValues(count)
    Array.tabulate(count)(index => values(index).toLong)

  private def permutation(count: Int): Array[Int] =
    val order = Array.tabulate(count)(identity)
    var state = shuffleSeed
    var index = count - 1
    while (index > 0) do
      state = (state * 1664525 + 1013904223) & 0xffffffffL
      val other = (state % (index + 1)).toInt
      val held = order(index)
      order(index) = order(other)
      order(other) = held
      index -= 1
    order

  private def readSettings(): Settings =
    Settings(
      query = sys.env.getOrElse("SPILL_SCALE_QUERY", "suite"),
      rows = readInt("SPILL_SCALE_ROWS", 10000000),
      spill = sys.env.get("SPILL_SCALE_SPILL").forall(Check.same(_, "true")),
      budget = readLong("SPILL_SCALE_BUDGET", defaultBudget),
      threads = readInt("SPILL_SCALE_THREADS", 4),
      warmup = readInt("SPILL_SCALE_WARMUP", 1),
      rounds = readInt("SPILL_SCALE_ROUNDS", 3),
    )

  private def readInt(name: String, default: Int): Int =
    sys.env.get(name).flatMap(_.toIntOption).getOrElse(default)

  private def readLong(name: String, default: Long): Long =
    sys.env.get(name).flatMap(_.toLongOption).getOrElse(default)

  private def deleteDirectory(directory: Path): Unit =
    if (Files.exists(directory)) then
      val stream = Files.list(directory)
      try stream.forEach(path => { val _ = Files.deleteIfExists(path) })
      finally stream.close()
      val _ = Files.deleteIfExists(directory)
