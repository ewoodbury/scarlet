package com.ewoodbury.scarlet.execution

import java.util.{Arrays, Locale}

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Times six foundation plans at one million rows.
 *
 * Checksums and batch counts run first, with a [[CountingSink]], and are not inside the timer. A
 * mismatch exits before any sample. Three warmup collects are discarded and nine are measured. The
 * p50 is the fifth of those nine, index 4. The timer wraps only `collect`.
 *
 * `to-string` and `flat-map` may be slower than twice the row path. That is `GAP` and exits 0.
 * `element-widen` does not lower. The row boundary still runs the int scan, so that scan
 * publishes. A defined `lower` is `FAIL`. The verdict stays `OK` when the ratio is above 2.
 *
 * {{{
 * sbt -J-Xmx3g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.FoundationProfile"
 * }}}
 */
object FoundationProfile:

  private val largeRows = 1000000
  private val warmup = 3
  private val measure = 9
  private val p50Index = 4
  private val names: Vector[String] =
    Vector("widen", "widen-sum", "same-map", "to-string", "flat-map", "element-widen")

  /** Checksums and batch rules at `rows`. Throws when a rule fails. Does not time. */
  def check(rows: Int): Unit =
    val found = observe(rows)
    found.foreach { one =>
      if !one.matched then
        throw new IllegalStateException(
          s"${one.name} columnar=${one.columnarBatches} row=${one.rowBatches} " +
            s"checksums ${one.columnarChecksum} ${one.rowChecksum}",
        )
    }

  /**
   * `FAIL` when the checksums differ or the batch rule fails. `GAP` when a matched query is more
   * than twice the row p50, except `element-widen`, which stays `OK`.
   */
  def judge(name: String, matched: Boolean, ratio: Double): String =
    if !matched then "FAIL"
    else
      name match
        case "element-widen" => "OK"
        case _ if ratio > 2.0 => "GAP"
        case _ => "OK"

  /** `1` when any verdict is `FAIL`. `GAP` stays `0`. */
  def exitCode(verdicts: Seq[String]): Int =
    if verdicts.contains("FAIL") then 1 else 0

  def main(args: Array[String]): Unit =
    if args.nonEmpty then
      System.err.println("usage: FoundationProfile")
      System.exit(1)
    val found = observe(largeRows)
    if found.exists(one => !one.matched) then
      found.foreach(one => println(untimed(one)))
      System.exit(exitCode(found.map(one => judge(one.name, one.matched, 1.0))))
    val lines = found.map(timeOne)
    lines.foreach(line => println(line))
    val code = exitCode(lines.map(verdictOf))
    if code != 0 then System.exit(code)

  private final case class Observed(
      name: String,
      columnarChecksum: Long,
      rowChecksum: Long,
      columnarBatches: Long,
      rowBatches: Long,
  ):
    def matched: Boolean =
      columnarChecksum == rowChecksum && batchesHold(columnarBatches, rowBatches)

  private def observe(rows: Int): Vector[Observed] =
    names.map(name => observeOne(name, rows))

  private def observeOne(name: String, rows: Int): Observed =
    name match
      case "widen" =>
        val data = widened(rows)
        val (columnarRows, columnarBatches) = collectPairsLong(columnar = true, data)
        val (rowRows, rowBatches) = collectPairsLong(columnar = false, data)
        Observed(
          name,
          sumPairsLong(columnarRows),
          sumPairsLong(rowRows),
          columnarBatches,
          rowBatches,
        )
      case "widen-sum" =>
        val data = widened(rows).reduceByKey[Int, Long]((left, right) => left + right)
        val (columnarRows, columnarBatches) = collectPairsLong(columnar = true, data)
        val (rowRows, rowBatches) = collectPairsLong(columnar = false, data)
        Observed(
          name,
          sumPairsLong(columnarRows),
          sumPairsLong(rowRows),
          columnarBatches,
          rowBatches,
        )
      case "same-map" =>
        val data = intPairs(rows).mapValues[Int, Int, Int](value => value + 1)
        val (columnarRows, columnarBatches) = collectPairsInt(columnar = true, data)
        val (rowRows, rowBatches) = collectPairsInt(columnar = false, data)
        Observed(
          name,
          sumPairsInt(columnarRows),
          sumPairsInt(rowRows),
          columnarBatches,
          rowBatches,
        )
      case "to-string" =>
        val data = intPairs(rows).mapValues[Int, Int, String](value => value.toString)
        val (columnarRows, columnarBatches) = collectPairsString(columnar = true, data)
        val (rowRows, rowBatches) = collectPairsString(columnar = false, data)
        Observed(
          name,
          sumPairsString(columnarRows),
          sumPairsString(rowRows),
          columnarBatches,
          rowBatches,
        )
      case "flat-map" =>
        val data = flatMapped(rows)
        val (columnarRows, columnarBatches) = collectInts(columnar = true, data)
        val (rowRows, rowBatches) = collectInts(columnar = false, data)
        Observed(name, sumInts(columnarRows), sumInts(rowRows), columnarBatches, rowBatches)
      case "element-widen" =>
        val data = ints(rows).map(value => value.toLong)
        if ColumnarPlanner.lower(data.plan).isDefined then
          throw new IllegalStateException("element-widen lowered")
        val (columnarRows, columnarBatches) = collectLongs(columnar = true, data)
        val (rowRows, rowBatches) = collectLongs(columnar = false, data)
        Observed(name, sumLongs(columnarRows), sumLongs(rowRows), columnarBatches, rowBatches)
      case other =>
        throw new IllegalStateException(s"unknown query $other")

  /**
   * Columnar layout publishes. Row layout does not.
   *
   * `element-widen` is included. The map stays unlowered and the scan under [[RowBoundary.map]]
   * publishes. Zero columnar batches would mean that child was dropped.
   */
  private def batchesHold(columnar: Long, row: Long): Boolean =
    columnar > 0L && row == 0L

  private def timeOne(one: Observed): String =
    val (columnarNanos, rowNanos) = p50s(one.name)
    if rowNanos == 0L then
      System.err.println(s"row p50 is 0 for ${one.name}")
      failRatio(one)
    else
      val ratio = columnarNanos.toDouble / rowNanos.toDouble
      val verdict = judge(one.name, one.matched, ratio)
      line(
        one,
        ms(columnarNanos),
        ms(rowNanos),
        String.format(Locale.US, "%.2f", Double.box(ratio)),
        verdict,
      )

  private def p50s(name: String): (Long, Long) =
    def pair[A](data: DistCollection[A]): (Long, Long) =
      val rowNanos = p50Nanos(columnar = false, data)
      val columnarNanos = if rowNanos == 0L then 0L else p50Nanos(columnar = true, data)
      (columnarNanos, rowNanos)
    name match
      case "widen" => pair(widened(largeRows))
      case "widen-sum" =>
        pair(widened(largeRows).reduceByKey[Int, Long]((left, right) => left + right))
      case "same-map" => pair(intPairs(largeRows).mapValues[Int, Int, Int](value => value + 1))
      case "to-string" =>
        pair(intPairs(largeRows).mapValues[Int, Int, String](value => value.toString))
      case "flat-map" => pair(flatMapped(largeRows))
      case "element-widen" => pair(ints(largeRows).map(value => value.toLong))
      case other => throw new IllegalStateException(s"unknown query $other")

  private def p50Nanos[A](columnar: Boolean, data: DistCollection[A]): Long =
    (0 until warmup).foreach(_ => discard(columnar, data))
    val samples = Array.fill(measure)(elapsed(columnar, data))
    Arrays.sort(samples)
    samples(p50Index)

  private def discard[A](columnar: Boolean, data: DistCollection[A]): Unit =
    val previous = ScarletConf.get
    try
      ScarletConf.set(previous.copy(columnarExecution = columnar))
      ScarletRuntime.get.shuffle.clear()
      val size = data.collect().size
      if size < 0 then throw new IllegalStateException("negative collect")
    finally ScarletConf.set(previous)

  private def elapsed[A](columnar: Boolean, data: DistCollection[A]): Long =
    val previous = ScarletConf.get
    try
      ScarletConf.set(previous.copy(columnarExecution = columnar))
      ScarletRuntime.get.shuffle.clear()
      val started = System.nanoTime()
      val size = data.collect().size
      val taken = System.nanoTime() - started
      if size < 0 then throw new IllegalStateException("negative collect")
      taken
    finally ScarletConf.set(previous)

  private def collectPairsLong(
      columnar: Boolean,
      data: DistCollection[(Int, Long)],
  ): (Seq[(Int, Long)], Long) =
    collected(columnar, data)

  private def collectPairsInt(
      columnar: Boolean,
      data: DistCollection[(Int, Int)],
  ): (Seq[(Int, Int)], Long) =
    collected(columnar, data)

  private def collectPairsString(
      columnar: Boolean,
      data: DistCollection[(Int, String)],
  ): (Seq[(Int, String)], Long) =
    collected(columnar, data)

  private def collectInts(columnar: Boolean, data: DistCollection[Int]): (Seq[Int], Long) =
    collected(columnar, data)

  private def collectLongs(columnar: Boolean, data: DistCollection[Long]): (Seq[Long], Long) =
    collected(columnar, data)

  private def collected[A](columnar: Boolean, data: DistCollection[A]): (Seq[A], Long) =
    val previous = ScarletConf.get
    try
      ScarletConf.set(previous.copy(columnarExecution = columnar))
      ScarletRuntime.get.shuffle.clear()
      ColumnMetrics.withSink(new CountingSink) {
        val rows = data.collect().toSeq
        (rows, ColumnMetrics.batchCount)
      }
    finally ScarletConf.set(previous)

  private def intPairs(rows: Int): DistCollection[(Int, Int)] =
    val keys = Array.tabulate(rows)(i => i % 1024)
    val values = Array.tabulate(rows)(i => i + 1)
    DistCollection.ofIntPairs(keys, values, 2)

  private def ints(rows: Int): DistCollection[Int] =
    DistCollection.ofInts(Array.tabulate(rows)(i => i + 1), 2)

  private def widened(rows: Int): DistCollection[(Int, Long)] =
    intPairs(rows).mapValues[Int, Int, Long](value => value.toLong)

  private def flatMapped(rows: Int): DistCollection[Int] =
    ints(rows)
      .filter(value => value % 2 == 0)
      .map(value => value * 2)
      .flatMap(value => Seq(value, value + 1))

  private def sumPairsLong(rows: Seq[(Int, Long)]): Long =
    rows.foldLeft(0L)((acc, row) => acc + row._1.toLong * 31L + row._2)

  private def sumPairsInt(rows: Seq[(Int, Int)]): Long =
    rows.foldLeft(0L)((acc, row) => acc + row._1.toLong * 31L + row._2.toLong)

  private def sumPairsString(rows: Seq[(Int, String)]): Long =
    rows.foldLeft(0L)((acc, row) => acc + row._1.toLong * 31L + row._2.hashCode.toLong)

  private def sumInts(rows: Seq[Int]): Long =
    rows.foldLeft(0L)((acc, value) => acc + value.toLong)

  private def sumLongs(rows: Seq[Long]): Long =
    rows.foldLeft(0L)((acc, value) => acc + value)

  private def ms(nanos: Long): String =
    String.format(Locale.US, "%.2f", Double.box(nanos.toDouble / 1000000.0))

  private def untimed(one: Observed): String =
    line(one, "0.00", "0.00", "0.00", judge(one.name, one.matched, 1.0))

  /** A zero row p50 has no ratio. The line is `FAIL` and the process exits. */
  private def failRatio(one: Observed): String =
    line(one, "0.00", "0.00", "na", "FAIL")

  private def line(
      one: Observed,
      columnarMs: String,
      rowMs: String,
      ratio: String,
      verdict: String,
  ): String =
    s"PROFILE query=${one.name} rows=$largeRows columnar_p50_ms=$columnarMs " +
      s"row_p50_ms=$rowMs ratio=$ratio columnar_batches=${one.columnarBatches} " +
      s"row_batches=${one.rowBatches} checksum=${one.columnarChecksum} verdict=$verdict"

  private def verdictOf(text: String): String =
    text.split("verdict=").lift(1).getOrElse("FAIL")
