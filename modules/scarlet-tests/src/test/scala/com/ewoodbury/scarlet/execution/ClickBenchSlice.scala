package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Nine ClickBench queries on 4096 generated rows. This is not the hits table.
 *
 * Numbers follow the 0-based order of `clickhouse/queries.sql`. An average is the long sum and the
 * count. Query 2's width average is 3679360 / 4096. Query 3's user average is 8386588672 / 4096.
 * The checker sorts query 7. The engine does not. [[note]] names every query.
 */
object ClickBenchSlice:

  val rowCount: Int = 4096
  val partitions: Int = 2
  val countAll: Long = 4096L
  val countAdv: Long = 3584L
  val sumAdv: Long = 14336L
  val sumWidth: Long = 3679360L
  val sumWidthPlus: Long = 3724416L
  val minWidth: Int = 800
  val maxWidth: Int = 999
  val widthExtremum: Long = 800L * 31L + 999L

  /** Rows whose generated phrase is present and not empty. */
  val notEmpty: Long = 2457L
  val sumUser: Long = 8386588672L
  val mixedSums: Long = 17583232L

  /** Keys 1 to 7, count 512, sum 512 * key, min and max the key. Count descending, then key. */
  val groupedMix: Long = -2156419963914273660L
  val groups: Long = 7L
  val twoKeyGroups: Long = 200L
  val twoKeyMix: Long = -5517106496331210496L

  val note: Vector[String] = Vector(
    "0 count. This file.",
    "1 count where AdvEngineID is not 0. This file.",
    "2 sum of AdvEngineID, count, and sum of ResolutionWidth. This file.",
    "3 sum of UserID. This file.",
    "4 distinct count of UserID. Missing: distinct count.",
    "5 distinct count of a string. Missing: string distinct.",
    "6 min and max of ResolutionWidth. This file.",
    "7 group by AdvEngineID. This file. The checker applies the order.",
    "8 distinct count by one int. Missing: distinct count.",
    "9 several aggregates and a distinct count. Missing: distinct count.",
    "10 string not equal to empty. This file.",
    "11 two strings in the group key. Missing: multi-column group.",
    "12 group by a string. Missing: string group key.",
    "13 distinct count by a string. Missing: string group key.",
    "14 int and string group key. Missing: multi-column group.",
    "15 group by a long, then sort and limit. Missing: sort.",
    "16 long and string group key. Missing: multi-column group.",
    "17 long and string group key. Missing: multi-column group.",
    "18 minute extracted from a timestamp. Missing: calendar arithmetic.",
    "19 long equality. Same compare as query 1. Not in this slice.",
    "20 string pattern match. Missing: string LIKE.",
    "21 pattern match and the min of a string. Missing: string LIKE.",
    "22 pattern match and a distinct count. Missing: string LIKE.",
    "23 pattern match, then sort. Missing: string LIKE.",
    "24 string filter, then sort and limit. Missing: string predicate.",
    "25 sort by a string. Missing: string predicate.",
    "26 sort by a time and a string. Missing: string predicate.",
    "27 length of a string. Missing: string length.",
    "28 regular expression replace. Missing: regex.",
    "29 add a literal to a column. This file.",
    "30 two-column group and a string filter. Missing: multi-column group.",
    "31 two-column group. Missing: multi-column group.",
    "32 two int keys. This file. The checker applies the order.",
    "33 group by a string. Missing: string group key.",
    "34 group by a constant and a string. Missing: string group key.",
    "35 group by a column expression. Missing: expression group key.",
    "36 date range and a string group. Missing: string group key.",
    "37 string group on a date range. Missing: string group key.",
    "38 string group and a flag filter. Missing: string group key.",
    "39 several group columns and a case. Missing: multi-column group.",
    "40 two-column group on a date range. Missing: multi-column group.",
    "41 two-column group on a date range. Missing: multi-column group.",
    "42 truncate a timestamp to a minute. Missing: calendar arithmetic.",
  )

  def main(args: Array[String]): Unit =
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(2)
    try
      ScarletConf.set(
        savedConf.copy(
          threadPoolSize = 2,
          columnarBatchSize = 8192,
          defaultShufflePartitions = partitions,
          columnarExecution = true,
          spillEnabled = false,
        ),
      )
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      val problems = failures()
      if problems.nonEmpty then
        System.err.println(s"clickbench slice failed: ${problems.mkString("; ")}")
        sys.exit(1)
    finally
      try scheduler.shutdown()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  /** Columnar lowering, then the same checksum on the row path. Empty means the slice passed. */
  def failures(): List[String] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = true, spillEnabled = false))
      prove() ++ bothPaths()
    finally ScarletConf.set(saved)

  def scan(): DistCollection[WideRow] =
    DistCollection.ofColumns(scanInputs(), partitions)

  private def scanInputs(): Vector[WideInput] =
    Vector(
      WideInput.ints("AdvEngineID", Array.tabulate(rowCount)(index => index % 8)),
      WideInput.ints("ResolutionWidth", Array.tabulate(rowCount)(index => 800 + (index % 200))),
      WideInput.longs("UserID", Array.tabulate(rowCount)(index => index.toLong * 1000L + 7L)),
    )

  def filtered(): DistCollection[WideRow] =
    scan().filterColumn(0, ColumnCompare(CompareOp.Ne, ColumnLiteral.Int32(0)))

  /** Query 10: `SearchPhrase <> ''`. A null phrase is dropped. The phrase is an extra column. */
  def notEmptyPhrase(): DistCollection[WideRow] =
    val phrase = WideInput.nullableStrings(
      "SearchPhrase",
      Array.tabulate(rowCount) { index =>
        if (index % 5 == 0) then "" else "p" + (index % 7)
      },
      Array.tabulate(rowCount)(index => index % 5 != 1),
    )
    val columns = scanInputs() :+ phrase
    DistCollection
      .ofColumns(columns, partitions)
      .filterColumn(3, ColumnCompare(CompareOp.Ne, ColumnLiteral.Utf8("")))

  def advAsLong(): DistCollection[WideRow] =
    scan().compute(ColumnExpr.Cast(0, ColumnKind.Int64), "adv64")

  def widthAsLong(): DistCollection[WideRow] =
    scan().compute(ColumnExpr.Cast(1, ColumnKind.Int64), "width64")

  /** Query 29: `ResolutionWidth + 11`, then a long cast. Columns land at ordinals 3, 4, and 5. */
  def widthPlus(): DistCollection[WideRow] =
    scan()
      .compute(ColumnExpr.Lit(ColumnLiteral.Int32(11)), "eleven")
      .compute(ColumnExpr.Add(1, 3), "widthPlus")
      .compute(ColumnExpr.Cast(4, ColumnKind.Int64), "widthPlus64")

  def grouped(): DistCollection[WideRow] =
    filtered().groupColumns(0, 0)

  /** Query 32 shape: two numeric keys, then count, sum, min, and max. */
  def groupedTwo(): DistCollection[WideRow] =
    scan().groupColumns(0, 1, 0)

  def groupedChecksum(rows: Iterable[WideRow]): Long =
    rows.toVector.sortWith(groupBefore).foldLeft(0L)(mixGroup)

  def twoKeyChecksum(rows: Iterable[WideRow]): Long =
    rows.toVector.sortWith(twoBefore).foldLeft(0L)(mixTwo)

  private def prove(): List[String] =
    List(
      counted("query 0", scan().plan, countAll),
      shaped("query 1", filtered().plan, "WideFilter0(Scan)"),
      counted("query 1", filtered().plan, countAdv),
      shaped("query 2 adv", advAsLong().plan, "WideCompute(Scan)"),
      summed("query 2 adv", advAsLong().plan, 3, sumAdv),
      summed("query 2 width", widthAsLong().plan, 3, sumWidth),
      summed("query 3", scan().plan, 2, sumUser),
      extremum("query 6 min", scan().plan, 1, minWidth, least = true),
      extremum("query 6 max", scan().plan, 1, maxWidth, least = false),
      shaped("query 10", notEmptyPhrase().plan, "WideFilter3(Scan)"),
      counted("query 10", notEmptyPhrase().plan, notEmpty),
      shaped("query 7", grouped().plan, "WideGroup(WideFilter0(Scan))"),
      counted("query 7", grouped().plan, groups),
      shaped("query 32", groupedTwo().plan, "WideGroup(Scan)"),
      counted("query 32", groupedTwo().plan, twoKeyGroups),
      shaped(
        "query 29",
        widthPlus().plan,
        "WideCompute(WideCompute(WideCompute(Scan)))",
      ),
      summed("query 29", widthPlus().plan, 5, sumWidthPlus),
    ).flatten

  private def bothPaths(): List[String] =
    List(
      both("query 0", countAll)(scan().count()),
      both("query 1", countAdv)(filtered().count()),
      both("query 2", mixedSums)(mixSums()),
      both("query 3", sumUser)(scan().sumLongColumn(2)),
      both("query 6", widthExtremum)(mixExtremum()),
      both("query 10", notEmpty)(notEmptyPhrase().count()),
      both("query 7", groupedMix)(groupedChecksum(grouped().collect())),
      both("query 32", twoKeyMix)(twoKeyChecksum(groupedTwo().collect())),
      both("query 29", sumWidthPlus)(widthPlus().sumLongColumn(5)),
    ).flatten

  private def mixExtremum(): Long =
    val min = scan().minIntColumn(1).toLong
    val max = scan().maxIntColumn(1).toLong
    min * 31L + max

  private def mixSums(): Long =
    val adv = advAsLong().sumLongColumn(3)
    val width = widthAsLong().sumLongColumn(3)
    val count = scan().count()
    ((adv * 31L + count) * 31L + width)

  private def both(name: String, expected: Long)(query: => Long): Option[String] =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    val columnar = query
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = query
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    if columnar != expected then Some(s"$name columnar $columnar expected $expected")
    else if rows != expected then Some(s"$name rows $rows expected $expected")
    else None

  private def counted(name: String, plan: Plan[WideRow], expected: Long): Option[String] =
    ColumnarPlanner.tryCount(plan) match
      case Some(value) if value == expected => None
      case Some(value) => Some(s"$name columnar count $value expected $expected")
      case None => Some(s"$name did not lower")

  private def summed(
      name: String,
      plan: Plan[WideRow],
      ordinal: Int,
      expected: Long,
  ): Option[String] =
    WideSum.longColumn(plan, ordinal) match
      case Some(value) if value == expected => None
      case Some(value) => Some(s"$name columnar sum $value expected $expected")
      case None => Some(s"$name sum did not lower")

  private def extremum(
      name: String,
      plan: Plan[WideRow],
      ordinal: Int,
      expected: Int,
      least: Boolean,
  ): Option[String] =
    val found =
      if (least) then WideExtremum.minInt(plan, ordinal) else WideExtremum.maxInt(plan, ordinal)
    found match
      case Some(value) if value == expected => None
      case Some(value) => Some(s"$name columnar $value expected $expected")
      case None => Some(s"$name did not lower")

  private def shaped(name: String, plan: Plan[WideRow], shape: String): Option[String] =
    ColumnarPlanner.lower(plan).map(_.shape) match
      case Some(found) if found == shape => None
      case Some(found) => Some(s"$name shape $found expected $shape")
      case None => Some(s"$name did not lower")

  private def groupBefore(left: WideRow, right: WideRow): Boolean =
    val leftCount = countOf(left)
    val rightCount = countOf(right)
    leftCount > rightCount || (leftCount == rightCount && keyOf(left) < keyOf(right))

  private def mixGroup(acc: Long, row: WideRow): Long = cells(row) match
    case Vector(
          Cell.Int32(key),
          Cell.Int64(count),
          Cell.Int64(sum),
          Cell.Int32(min),
          Cell.Int32(max),
        ) =>
      ((((acc * 31L + key.toLong) * 31L + count) * 31L + sum) * 31L + min.toLong) * 31L +
        max.toLong
    case other =>
      refuse(other.size)

  private def twoBefore(left: WideRow, right: WideRow): Boolean =
    val leftKey = intAt(left, 0)
    val rightKey = intAt(right, 0)
    leftKey < rightKey || (leftKey == rightKey && intAt(left, 1) < intAt(right, 1))

  private def mixTwo(acc: Long, row: WideRow): Long = cells(row) match
    case Vector(
          Cell.Int32(key),
          Cell.Int32(key2),
          Cell.Int64(count),
          Cell.Int64(sum),
          Cell.Int32(min),
          Cell.Int32(max),
        ) =>
      (((((acc * 31L + key.toLong) * 31L + key2.toLong) * 31L + count) * 31L + sum) * 31L +
        min.toLong) * 31L + max.toLong
    case other =>
      refuse(other.size)

  private def intAt(row: WideRow, ordinal: Int): Int = cells(row)(ordinal) match
    case Cell.Int32(value) => value
    case _ => refuse(cells(row).size)

  private def keyOf(row: WideRow): Int = cells(row) match
    case Vector(Cell.Int32(key), _, _, _, _) => key
    case other => refuse(other.size)

  private def countOf(row: WideRow): Long = cells(row) match
    case Vector(_, Cell.Int64(count), _, _, _) => count
    case other => refuse(other.size)

  private def cells(row: WideRow): Vector[Cell] = row.cells

  private def refuse(width: Int): Nothing =
    throw new IllegalArgumentException(s"group row has $width cells")
