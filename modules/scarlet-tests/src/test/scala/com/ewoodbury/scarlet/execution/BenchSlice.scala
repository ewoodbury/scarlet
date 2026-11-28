package com.ewoodbury.scarlet.execution

import java.util.Arrays

import scala.io.Source
import scala.util.Using

import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Times the plans [[ClickBenchSlice]] and [[TpchSlice]] already run.
 *
 * Not part of `make test`. `perf.yml` stays the five-minute checksum smoke. This main warms each
 * plan twice, measures it five times, and prints the median in milliseconds and rows per second.
 * Rows are the slice input size. ClickBench uses [[ClickBenchSlice.rowCount]]. TPC-H lineitem
 * plans use [[TpchSlice.lineitemRows]]. The orders join uses [[TpchSlice.orderRows]]. The small
 * fixtures use their largest scan.
 *
 * A missing `bench-slice-budget.txt` records the table and does not fail on time. A present file
 * fails when a median is above twice its budget. The pinned checksums stay. A mismatch fails. This
 * bench does not replace a checksum.
 *
 * `runMain` executes in the sbt JVM. `Test / fork` is left false, so absolute times carry sbt and
 * JIT noise.
 */
object BenchSlice:

  private val warmup: Int = 2
  private val rounds: Int = 5
  private val threads: Int = 2
  private val batchSize: Int = 8192
  private val budgetName: String = "bench-slice-budget.txt"

  /**
   * Largest scan in that plan: local orders 4, open orders 6, late lines 6, suppliers 3, offers 7.
   */
  private val localRows: Int = 4
  private val openRows: Int = 6
  private val lateRows: Int = 6
  private val supplierRows: Int = 3
  private val offerRows: Int = 7

  def main(args: Array[String]): Unit =
    if runBody() then sys.exit(1)

  private final case class SliceCase(name: String, rows: Int, expected: Long, action: () => Long)

  private def runBody(): Boolean =
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(threads)
    try
      ScarletConf.set(
        savedConf.copy(
          threadPoolSize = threads,
          columnarBatchSize = batchSize,
          defaultShufflePartitions = ClickBenchSlice.partitions,
          columnarExecution = true,
          spillEnabled = false,
        ),
      )
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      val problems = run()
      if problems.nonEmpty then
        System.err.println(s"bench slice failed: ${problems.mkString("; ")}")
        true
      else false
    finally
      try scheduler.shutdown()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  private def run(): List[String] =
    loadBudget() match
      case Left(message) => List(message)
      case Right(budget) =>
        val items = cases()
        val mode = if budget.isDefined then "enforced" else "absent"
        println(s"BENCH budget=$mode cases=${items.size} warmup=$warmup rounds=$rounds")
        timeAll(budget, items) ++ unknownBudgets(budget, items)

  private def timeAll(
      budget: Option[Map[String, Double]],
      pending: Vector[SliceCase],
  ): List[String] =
    pending.headOption match
      case None => Nil
      case Some(item) =>
        warm(item) match
          case Some(message) => List(message)
          case None =>
            measure(item) match
              case Left(message) => List(message)
              case Right(medianMs) =>
                println(formatLine(item, medianMs))
                overBudget(item.name, medianMs, budget).toList ++ timeAll(budget, pending.drop(1))

  private def warm(item: SliceCase): Option[String] =
    (0 until warmup).iterator
      .map(_ => timeOne(item))
      .collectFirst { case Left(message) => message }

  private def measure(item: SliceCase): Either[String, Double] =
    val samples = new Array[Double](rounds)
    val problem = (0 until rounds).iterator
      .map { index =>
        timeOne(item).map { millis =>
          samples(index) = millis
        }
      }
      .collectFirst { case Left(message) => message }
    problem match
      case Some(message) => Left[String, Double](message)
      case None => Right[String, Double](median(samples))

  private def timeOne(item: SliceCase): Either[String, Double] =
    ScarletRuntime.get.shuffle.clear()
    val started = System.nanoTime()
    val value = item.action()
    val elapsed = (System.nanoTime() - started) / 1_000_000.0
    if value == item.expected then Right[String, Double](elapsed)
    else Left[String, Double](s"${item.name} checksum $value expected ${item.expected}")

  private def median(samples: Array[Double]): Double =
    Arrays.sort(samples)
    samples(samples.length / 2)

  private def formatLine(item: SliceCase, medianMs: Double): String =
    val rate = item.rows.toDouble * 1000.0 / math.max(medianMs, 0.001)
    "BENCH name=" + item.name + " median_ms=" + fixed(medianMs) + " rows_per_sec=" +
      fixed(rate) + " rows=" + item.rows.toString

  private def fixed(value: Double): String =
    val negative = value < 0.0
    val scaled = math.round(math.abs(value) * 1000.0)
    val whole = (scaled / 1000L).toString
    val digits = (scaled % 1000L).toInt.toString
    val padded =
      if digits.length >= 3 then digits
      else if digits.length == 2 then "0" + digits
      else "00" + digits
    val sign = if negative then "-" else ""
    sign + whole + "." + padded

  private def overBudget(
      name: String,
      medianMs: Double,
      budget: Option[Map[String, Double]],
  ): Option[String] =
    budget match
      case None => None
      case Some(table) =>
        table.get(name) match
          case None => Some(s"$name has no budget")
          case Some(limit) if medianMs > limit * 2.0 =>
            Some(
              name + " median " + fixed(medianMs) + " ms is above twice " + fixed(limit) + " ms",
            )
          case Some(_) => None

  private def unknownBudgets(
      budget: Option[Map[String, Double]],
      items: Vector[SliceCase],
  ): List[String] =
    budget match
      case None => Nil
      case Some(table) =>
        val names = items.map(_.name).toSet
        val unknown = table.keySet.diff(names).toList.sorted
        if unknown.isEmpty then Nil else List(s"unknown budget ${unknown.mkString(", ")}")

  private def loadBudget(): Either[String, Option[Map[String, Double]]] =
    resource(budgetName) match
      case None => Right[String, Option[Map[String, Double]]](None)
      case Some(text) => parseBudgets(text).map(parsed => Some(parsed))

  private def resource(name: String): Option[String] =
    Option(getClass.getClassLoader.getResourceAsStream(name)).map { stream =>
      Using.resource(Source.fromInputStream(stream, "UTF-8"))(_.mkString)
    }

  private def parseBudgets(text: String): Either[String, Map[String, Double]] =
    val kept = text
      .replace("\r\n", "\n")
      .split("\n")
      .toVector
      .map(_.trim)
      .filter(line => line.nonEmpty && !line.startsWith("#"))
    val parsed = kept.map(parseBudget)
    val bad = parsed.collect { case Left(message) => message }
    if bad.nonEmpty then Left[String, Map[String, Double]](bad.mkString("; "))
    else
      val pairs = parsed.collect { case Right(pair) => pair }
      val names = pairs.map { case (name, _) => name }
      val duplicate = names.diff(names.distinct)
      if duplicate.nonEmpty then
        Left[String, Map[String, Double]](s"duplicate budget ${duplicate.distinct.mkString(", ")}")
      else Right[String, Map[String, Double]](pairs.toMap)

  private def parseBudget(line: String): Either[String, (String, Double)] =
    line.split("\\s+").toList match
      case name :: millis :: Nil =>
        millis.toDoubleOption match
          case Some(value) if value > 0.0 =>
            Right[String, (String, Double)]((name, value))
          case _ => Left[String, (String, Double)](s"budget millis [$millis]")
      case _ => Left[String, (String, Double)](s"budget line [$line]")

  private def mixSums(): Long =
    val adv = ClickBenchSlice.advAsLong().sumLongColumn(3)
    val width = ClickBenchSlice.widthAsLong().sumLongColumn(3)
    val count = ClickBenchSlice.scan().count()
    (adv * 31L + count) * 31L + width

  private def mixExtremum(): Long =
    val min = ClickBenchSlice.scan().minIntColumn(1).toLong
    val max = ClickBenchSlice.scan().maxIntColumn(1).toLong
    min * 31L + max

  private def cases(): Vector[SliceCase] =
    val click = ClickBenchSlice.rowCount
    val lines = TpchSlice.lineitemRows
    val orders = TpchSlice.orderRows
    Vector(
      SliceCase(
        "clickbench-0",
        click,
        ClickBenchSlice.countAll,
        () => ClickBenchSlice.scan().count(),
      ),
      SliceCase(
        "clickbench-1",
        click,
        ClickBenchSlice.countAdv,
        () => ClickBenchSlice.filtered().count(),
      ),
      SliceCase(
        "clickbench-19",
        click,
        ClickBenchSlice.matchedId,
        () => ClickBenchSlice.matchedUser().sumLongColumn(2),
      ),
      SliceCase(
        "clickbench-33",
        click,
        ClickBenchSlice.urlMix,
        () => ClickBenchSlice.phraseChecksum(ClickBenchSlice.groupedUrl().collect()),
      ),
      SliceCase("clickbench-2", click, ClickBenchSlice.mixedSums, () => mixSums()),
      SliceCase(
        "clickbench-3",
        click,
        ClickBenchSlice.sumUser,
        () => ClickBenchSlice.scan().sumLongColumn(2),
      ),
      SliceCase(
        "clickbench-4",
        click,
        ClickBenchSlice.distinctUsers,
        () => ClickBenchSlice.scan().countDistinctColumn(2),
      ),
      SliceCase(
        "clickbench-5",
        click,
        ClickBenchSlice.distinctPhrases,
        () => ClickBenchSlice.phrases().countDistinctColumn(3),
      ),
      SliceCase("clickbench-6", click, ClickBenchSlice.widthExtremum, () => mixExtremum()),
      SliceCase(
        "clickbench-10",
        click,
        ClickBenchSlice.notEmpty,
        () => ClickBenchSlice.notEmptyPhrase().count(),
      ),
      SliceCase(
        "clickbench-7",
        click,
        ClickBenchSlice.groupedMix,
        () => ClickBenchSlice.groupedChecksum(ClickBenchSlice.grouped().collect()),
      ),
      SliceCase(
        "clickbench-8",
        click,
        ClickBenchSlice.distinctMix,
        () => ClickBenchSlice.distinctChecksum(ClickBenchSlice.groupedDistinct().collect()),
      ),
      SliceCase(
        "clickbench-32",
        click,
        ClickBenchSlice.twoKeyMix,
        () => ClickBenchSlice.twoKeyChecksum(ClickBenchSlice.groupedTwo().collect()),
      ),
      SliceCase(
        "clickbench-12",
        click,
        ClickBenchSlice.phraseMix,
        () => ClickBenchSlice.phraseChecksum(ClickBenchSlice.groupedPhrase().collect()),
      ),
      SliceCase(
        "clickbench-11",
        click,
        ClickBenchSlice.twoStringMix,
        () => ClickBenchSlice.phoneChecksum(ClickBenchSlice.groupedPhones().collect()),
      ),
      SliceCase(
        "clickbench-14",
        click,
        ClickBenchSlice.intStringMix,
        () => ClickBenchSlice.intStringChecksum(ClickBenchSlice.groupedIntString().collect()),
      ),
      SliceCase(
        "clickbench-16",
        click,
        ClickBenchSlice.longStringMix,
        () => ClickBenchSlice.longStringChecksum(ClickBenchSlice.groupedLongString().collect()),
      ),
      SliceCase(
        "clickbench-15",
        click,
        ClickBenchSlice.topUserMix,
        () => ClickBenchSlice.topUserChecksum(ClickBenchSlice.topUsers().collect()),
      ),
      SliceCase(
        "clickbench-29",
        click,
        ClickBenchSlice.sumWidthPlus,
        () => ClickBenchSlice.widthPlus().sumLongColumn(5),
      ),
      SliceCase(
        "clickbench-20",
        click,
        ClickBenchSlice.likeUrlsCount,
        () => ClickBenchSlice.likeUrls().count(),
      ),
      SliceCase(
        "clickbench-21",
        click,
        ClickBenchSlice.stringMinMix,
        () => ClickBenchSlice.stringGroupChecksum(ClickBenchSlice.likeGroups().collect()),
      ),
      SliceCase(
        "clickbench-22",
        click,
        ClickBenchSlice.twoMinMix,
        () => ClickBenchSlice.twoMinChecksum(ClickBenchSlice.searchGroups().collect()),
      ),
      SliceCase(
        "clickbench-9",
        click,
        ClickBenchSlice.regionMix,
        () => ClickBenchSlice.regionChecksum(ClickBenchSlice.groupedRegion().collect()),
      ),
      SliceCase(
        "clickbench-23",
        click,
        ClickBenchSlice.likeSortMix,
        () => ClickBenchSlice.userChecksum(ClickBenchSlice.likeSorted().collect()),
      ),
      SliceCase(
        "clickbench-24",
        click,
        ClickBenchSlice.phraseFilterSortMix,
        () => ClickBenchSlice.userPhraseChecksum(ClickBenchSlice.filteredPhrases().collect()),
      ),
      SliceCase(
        "clickbench-25",
        click,
        ClickBenchSlice.phraseSortMix,
        () => ClickBenchSlice.userPhraseChecksum(ClickBenchSlice.sortedPhrases().collect()),
      ),
      SliceCase(
        "clickbench-26",
        click,
        ClickBenchSlice.timedPhraseMix,
        () => ClickBenchSlice.timedPhraseChecksum(ClickBenchSlice.timedPhrases().collect()),
      ),
      SliceCase(
        "clickbench-28",
        click,
        ClickBenchSlice.hostMix,
        () => ClickBenchSlice.hostChecksum(ClickBenchSlice.hostReferers().collect()),
      ),
      SliceCase(
        "clickbench-27",
        click,
        ClickBenchSlice.urlLengthSum,
        () => ClickBenchSlice.urlLengths().sumLongColumn(5),
      ),
      SliceCase(
        "clickbench-35",
        click,
        ClickBenchSlice.exprGroupMix,
        () => ClickBenchSlice.groupedChecksum(ClickBenchSlice.widthMinus().collect()),
      ),
      SliceCase(
        "clickbench-18",
        click,
        ClickBenchSlice.minuteMix,
        () => ClickBenchSlice.groupedChecksum(ClickBenchSlice.byMinute().collect()),
      ),
      SliceCase(
        "clickbench-42",
        click,
        ClickBenchSlice.truncMix,
        () => ClickBenchSlice.longGroupChecksum(ClickBenchSlice.byMinuteStart().collect()),
      ),
      SliceCase(
        "tpch-6",
        lines,
        TpchSlice.revenue,
        () => TpchSlice.revenuePlan().sumLongColumn(4),
      ),
      SliceCase(
        "tpch-1",
        lines,
        TpchSlice.pricingMix,
        () => TpchSlice.pricingChecksum(TpchSlice.pricingPlan().collect()),
      ),
      SliceCase(
        "tpch-3",
        orders,
        TpchSlice.shippingMix,
        () => TpchSlice.shippingChecksum(TpchSlice.shippingPlan().collect()),
      ),
      SliceCase(
        "tpch-5",
        localRows,
        TpchSlice.localMix,
        () => TpchSlice.localChecksum(TpchSlice.localPlan().collect()),
      ),
      SliceCase(
        "tpch-13",
        openRows,
        TpchSlice.outerMix,
        () => TpchSlice.outerChecksum(TpchSlice.custOrderPlan().collect()),
      ),
      SliceCase(
        "tpch-4",
        lateRows,
        TpchSlice.priorityMix,
        () => TpchSlice.priorityChecksum(TpchSlice.priorityPlan().collect()),
      ),
      SliceCase(
        "tpch-15",
        supplierRows,
        TpchSlice.topMix,
        () => TpchSlice.topChecksum(TpchSlice.topPlan().collect()),
      ),
      SliceCase(
        "tpch-2",
        offerRows,
        TpchSlice.minCostMix,
        () => TpchSlice.minCostChecksum(TpchSlice.minCostPlan().collect()),
      ),
    )
