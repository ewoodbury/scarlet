package com.ewoodbury.scarlet.execution
import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler
import com.ewoodbury.scarlet.support.Check

/**
 * Twenty-six ClickBench queries on 4096 generated rows. This is not the hits table.
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

  /** Ten largest UserIDs, each a group of one. Folded in engine order. */
  val topUserMix: Long = -7996923543694224240L

  /** Phrase `p0` to `p3`, each 1024 rows. Sorted by the phrase, then folded. */
  val phraseMix: Long = 7617713573459572694L

  /** Phone `a` or `b`, model `c`, `d`, or `e`. Six groups. */
  val twoStringMix: Long = 3618175579426930483L

  /** AdvEngineID with phrase `s0` to `s2`. Twenty-four groups. */
  val intStringMix: Long = -8865579991621256728L

  /** Each UserID with its phrase. One row per group. */
  val longStringMix: Long = 4644043001656397824L
  val phraseGroups: Long = 4L
  val phoneGroups: Long = 6L
  val intStringGroups: Long = 24L
  val groups: Long = 7L
  val twoKeyGroups: Long = 200L
  val twoKeyMix: Long = -5517106496331210496L

  /** Every generated UserID is unique, so the distinct count equals the row count. */
  val distinctUsers: Long = 4096L

  /** Phrase `p0` to `p3`. */
  val distinctPhrases: Long = 4L

  /** Widths inside one AdvEngineID. The group count is 512. */
  val distinctWidths: Long = 25L
  val distinctGroups: Long = 8L
  val distinctMix: Long = 6945918260932055340L

  /** Present URLs matching `%google%`. Null URLs are dropped. */
  val likeUrlsCount: Long = 2315L

  /** UTF-16 length of every present URL. A null adds nothing. */
  val urlLengthSum: Long = 63655L

  /** Ten largest UserIDs among the LIKE matches, in engine order. */
  val likeSortMix: Long = 1088793507528100824L

  /** Phrase groups of the LIKE matches. The checker applies the order. Min is `agoogle`. */
  val stringMinMix: Long = 859446720053382657L

  /** Ten smallest non-empty phrases, in engine order. */
  val phraseSortMix: Long = -5808577833312032032L

  /** Ten largest UserIDs whose phrase is greater than `p1`, in engine order. */
  val phraseFilterSortMix: Long = 7829478632560354530L

  /**
   * Query 35 adapted to one key: `ResolutionWidth - 1`. Two hundred groups. The checker sorts by
   * count, then by the key. The published statement has four keys.
   */
  val exprGroupMix: Long = 9176981150844157028L
  val exprGroups: Long = 200L

  /** Query 18 adapted to one key: minute of an int64 epoch. The checker sorts. */
  val minuteMix: Long = -4045389298186771610L
  val minuteGroups: Long = 60L

  /** Query 42 adapted to one key: the epoch truncated to a minute. The checker sorts. */
  val truncMix: Long = -2455919995862346808L
  val truncGroups: Long = 69L

  val note: Vector[String] = Vector(
    "0 count. This file.",
    "1 count where AdvEngineID is not 0. This file.",
    "2 sum of AdvEngineID, count, and sum of ResolutionWidth. This file.",
    "3 sum of UserID. This file.",
    "4 distinct count of UserID. This file.",
    "5 distinct count of a string. This file.",
    "6 min and max of ResolutionWidth. This file.",
    "7 group by AdvEngineID. This file. The checker applies the order.",
    "8 distinct count by one int. This file. The checker applies the order.",
    "9 sums AdvEngineID and averages ResolutionWidth, then a distinct count. Missing: a second measure.",
    "10 string not equal to empty. This file.",
    "11 two strings in the group key. This file. The checker applies the order.",
    "12 group by a string. This file. The checker applies the order.",
    "13 distinct count by a string. Missing: distinct count.",
    "14 int and string group key. This file. The checker applies the order.",
    "15 group by a long, then sort and limit. This file.",
    "16 long and string group key. This file. The checker applies the order.",
    "17 long and string group key. Same keys as query 16. Limit without an order waits.",
    "18 minute of an int64 epoch. This file. The checker applies the order.",
    "19 long equality. Same compare as query 1. Not in this slice.",
    "20 string pattern match. This file.",
    "21 pattern match and the min of a string. This file. The checker applies the order.",
    "22 pattern match, two string mins, and a distinct count. Missing: a second string min.",
    "23 pattern match, then sort. This file.",
    "24 string filter, then sort and limit. This file.",
    "25 sort by a string. This file.",
    "26 sort by a time and a string. Missing: a second sort key.",
    "27 length of a string. This file.",
    "28 regular expression replace. Missing: regex.",
    "29 add a literal to a column. This file.",
    "30 two-column group and a string filter. Missing: multi-column group.",
    "31 two-column group. Missing: multi-column group.",
    "32 two int keys. This file. The checker applies the order.",
    "33 group by a string. Same key as query 12. Not in this slice.",
    "34 group by a constant and a string. Missing: string group key.",
    "35 group by one int expression. This file. The checker applies the order.",
    "36 date range and a string group. Missing: a date literal.",
    "37 string group on a date range. Missing: a date literal.",
    "38 string group and a flag filter. Missing: a date literal.",
    "39 several group columns and a case. Missing: multi-column group.",
    "40 two-column group on a date range. Missing: a date literal.",
    "41 two-column group on a date range. Missing: a date literal.",
    "42 truncate an int64 epoch to a minute. This file. The checker applies the order.",
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

  private[execution] def scanInputs(): Vector[WideInput] =
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

  /** Query 20: `URL LIKE '%google%'`. */
  def likeUrls(): DistCollection[WideRow] =
    urls().filterColumn(3, ColumnCompare(CompareOp.Like, ColumnLiteral.Utf8("%google%")))

  /**
   * Query 21: LIKE, then the min of that URL by phrase. The phrase is never empty. Each phrase
   * sees `agoogle` and a longer URL, so the min is not the first dictionary code.
   */
  def likeGroups(): DistCollection[WideRow] =
    phrasesAndUrls()
      .filterColumn(4, ColumnCompare(CompareOp.Like, ColumnLiteral.Utf8("%google%")))
      .filterColumn(3, ColumnCompare(CompareOp.Ne, ColumnLiteral.Utf8("")))
      .groupColumns(3, 4)

  /** Query 23: LIKE, then the long UserID standing in for EventTime, then ten rows. */
  def likeSorted(): DistCollection[WideRow] =
    likeUrls().sortColumn(2, descending = true).limitColumns(10)

  /** Query 24: a string filter, then the long UserID sort, then ten rows. */
  def filteredPhrases(): DistCollection[WideRow] =
    notEmptyPhrase()
      .filterColumn(3, ColumnCompare(CompareOp.Gt, ColumnLiteral.Utf8("p1")))
      .sortColumn(2, descending = true)
      .limitColumns(10)

  /** Query 25: non-empty phrases, sorted by the phrase, then ten rows. */
  def sortedPhrases(): DistCollection[WideRow] =
    notEmptyPhrase().sortColumn(3, descending = false).limitColumns(10)

  /** Query 27: sum of the UTF-16 length. Average and HAVING stay out. */
  def urlLengths(): DistCollection[WideRow] =
    urls()
      .compute(ColumnExpr.Len(3), "n")
      .compute(ColumnExpr.Cast(4, ColumnKind.Int64), "n64")

  def urls(): DistCollection[WideRow] =
    DistCollection.ofColumns(scanInputs() :+ urlColumn(), partitions)

  def phrasesAndUrls(): DistCollection[WideRow] =
    DistCollection.ofColumns(scanInputs() :+ phraseColumn("p", 4) :+ urlColumn(), partitions)

  def stringGroupChecksum(rows: Iterable[WideRow]): Long =
    rows.toVector.sortWith(stringGroupBefore).foldLeft(0L)(mixStringGroup)

  def userPhraseChecksum(rows: Iterable[WideRow]): Long =
    rows.foldLeft(0L)(mixUserPhrase)

  def userChecksum(rows: Iterable[WideRow]): Long =
    rows.foldLeft(0L)((acc, row) => acc * 31L + longAt(row, 2))

  def advAsLong(): DistCollection[WideRow] =
    scan().compute(ColumnExpr.Cast(0, ColumnKind.Int64), "adv64")

  def widthAsLong(): DistCollection[WideRow] =
    scan().compute(ColumnExpr.Cast(1, ColumnKind.Int64), "width64")

  /**
   * Query 18: minute of `EventTime`, then a group on that column. The published statement has
   * three keys.
   */
  def byMinute(): DistCollection[WideRow] =
    events().compute(ColumnExpr.Minute(3), "minute").groupColumns(4, 4)

  /**
   * Query 42: `EventTime` truncated to a minute, then a group. The published statement has a date
   * filter and an offset.
   */
  def byMinuteStart(): DistCollection[WideRow] =
    events().compute(ColumnExpr.TruncMinute(3), "minuteStart").groupColumns(4, 4)

  def events(): DistCollection[WideRow] =
    DistCollection.ofColumns(
      scanInputs() :+ WideInput.longs(
        "EventTime",
        Array.tabulate(rowCount)(index => index.toLong),
      ),
      partitions,
    )

  def longGroupChecksum(rows: Iterable[WideRow]): Long =
    rows.toVector.sortWith(longGroupBefore).foldLeft(0L)(mixLong)

  /**
   * Query 35: `ResolutionWidth - 1`, then a group on that column. The key is ordinal 4. The
   * published statement groups four expressions. This slice keeps one.
   */
  def widthMinus(): DistCollection[WideRow] =
    scan()
      .compute(ColumnExpr.Lit(ColumnLiteral.Int32(1)), "one")
      .compute(ColumnExpr.Sub(1, 3), "widthMinus")
      .groupColumns(4, 4)

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

  /** Query 12: one string key, measure AdvEngineID. */
  def groupedPhrase(): DistCollection[WideRow] =
    phrases().groupColumns(3, 0)

  /** Query 11: two string keys. */
  def groupedPhones(): DistCollection[WideRow] =
    phones().groupColumns(3, 4, 0)

  /** Query 14: an int key and a string key. Measure ResolutionWidth. */
  def groupedIntString(): DistCollection[WideRow] =
    tagged().groupColumns(0, 3, 1)

  /** Query 16: a long key and a string key. */
  def groupedLongString(): DistCollection[WideRow] =
    phrases().groupColumns(2, 3, 0)

  /**
   * Query 8 shape: group AdvEngineID and count distinct ResolutionWidth. The published statement
   * groups RegionID and orders by the distinct count. This slice has no RegionID. Eight groups
   * make the limit a no-op. The checker applies the order.
   */
  def groupedDistinct(): DistCollection[WideRow] =
    scan().groupDistinct(0, 1, 1)

  def distinctChecksum(rows: Iterable[WideRow]): Long =
    rows.toVector.sortWith(distinctBefore).foldLeft(0L)(mixDistinct)

  def phrases(): DistCollection[WideRow] =
    DistCollection.ofColumns(scanInputs() :+ phraseColumn("p", 4), partitions)

  def phraseChecksum(rows: Iterable[WideRow]): Long =
    rows.toVector.sortBy(row => textAt(row, 0)).foldLeft(0L)(mixPhrase)

  def phoneChecksum(rows: Iterable[WideRow]): Long =
    rows.toVector.sortWith(textBefore).foldLeft(0L)(mixTexts)

  def intStringChecksum(rows: Iterable[WideRow]): Long =
    rows.toVector.sortWith(intTextBefore).foldLeft(0L)(mixIntText)

  def longStringChecksum(rows: Iterable[WideRow]): Long =
    rows.toVector.sortWith(longTextBefore).foldLeft(0L)(mixLongText)

  private def phones(): DistCollection[WideRow] =
    val phone = WideInput.strings(
      "MobilePhone",
      Array.tabulate(rowCount)(index => if index % 2 == 0 then "a" else "b"),
    )
    val model = WideInput.strings(
      "MobilePhoneModel",
      Array.tabulate(rowCount)(index => "cde".charAt(index % 3).toString),
    )
    DistCollection.ofColumns(scanInputs() :+ phone :+ model, partitions)

  private def tagged(): DistCollection[WideRow] =
    DistCollection.ofColumns(scanInputs() :+ phraseColumn("s", 3), partitions)

  private def urlColumn(): WideInput =
    WideInput.nullableStrings(
      "URL",
      Array.tabulate(rowCount)(urlText),
      Array.tabulate(rowCount)(index => index % 17 != 0),
    )

  private def urlText(index: Int): String =
    val kind = (index / 4) % 5
    if kind == 0 then "https://www.google.com/search"
    else if kind == 1 then "https://news.google.com/a"
    else if kind == 2 then "agoogle"
    else if kind == 3 then "https://example.com/"
    else if index % 8 < 4 then "é"
    else "e\u0301"

  private def phraseColumn(prefix: String, width: Int): WideInput =
    WideInput.strings(
      "SearchPhrase",
      Array.tabulate(rowCount)(index => prefix + (index % width).toString),
    )

  /**
   * Query 15 shape: group UserID, sort that key descending, then ten rows. The published statement
   * sorts by count. Every count is 1, so this slice sorts the key.
   */
  def topUsers(): DistCollection[WideRow] =
    scan().groupColumns(2, 2).sortColumn(0, descending = true).limitColumns(10)

  def topUserChecksum(rows: Iterable[WideRow]): Long =
    rows.foldLeft(0L)(mixLong)

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
      distinct("query 4", scan().plan, 2, distinctUsers),
      distinct("query 5", phrases().plan, 3, distinctPhrases),
      extremum("query 6 min", scan().plan, 1, minWidth, least = true),
      extremum("query 6 max", scan().plan, 1, maxWidth, least = false),
      shaped("query 10", notEmptyPhrase().plan, "WideFilter3(Scan)"),
      counted("query 10", notEmptyPhrase().plan, notEmpty),
      shaped("query 7", grouped().plan, "WideGroup(WideFilter0(Scan))"),
      counted("query 7", grouped().plan, groups),
      shaped("query 8", groupedDistinct().plan, "WideGroup(Scan)"),
      counted("query 8", groupedDistinct().plan, distinctGroups),
      shaped("query 32", groupedTwo().plan, "WideGroup(Scan)"),
      counted("query 32", groupedTwo().plan, twoKeyGroups),
      shaped("query 12", groupedPhrase().plan, "WideGroup(Scan)"),
      counted("query 12", groupedPhrase().plan, phraseGroups),
      shaped("query 11", groupedPhones().plan, "WideGroup(Scan)"),
      counted("query 11", groupedPhones().plan, phoneGroups),
      shaped("query 14", groupedIntString().plan, "WideGroup(Scan)"),
      counted("query 14", groupedIntString().plan, intStringGroups),
      shaped("query 16", groupedLongString().plan, "WideGroup(Scan)"),
      counted("query 16", groupedLongString().plan, rowCount.toLong),
      shaped("query 15", topUsers().plan, "WideLimit(WideSort(WideGroup(Scan)))"),
      shaped(
        "query 29",
        widthPlus().plan,
        "WideCompute(WideCompute(WideCompute(Scan)))",
      ),
      summed("query 29", widthPlus().plan, 5, sumWidthPlus),
      shaped("query 20", likeUrls().plan, "WideFilter3(Scan)"),
      counted("query 20", likeUrls().plan, likeUrlsCount),
      shaped("query 21", likeGroups().plan, "WideGroup(WideFilter3(WideFilter4(Scan)))"),
      counted("query 21", likeGroups().plan, 4L),
      shaped("query 23", likeSorted().plan, "WideLimit(WideSort(WideFilter3(Scan)))"),
      counted("query 23", likeSorted().plan, 10L),
      shaped(
        "query 24",
        filteredPhrases().plan,
        "WideLimit(WideSort(WideFilter3(WideFilter3(Scan))))",
      ),
      counted("query 24", filteredPhrases().plan, 10L),
      shaped("query 25", sortedPhrases().plan, "WideLimit(WideSort(WideFilter3(Scan)))"),
      counted("query 25", sortedPhrases().plan, 10L),
      shaped("query 27", urlLengths().plan, "WideCompute(WideCompute(Scan))"),
      summed("query 27", urlLengths().plan, 5, urlLengthSum),
      shaped("query 35", widthMinus().plan, "WideGroup(WideCompute(WideCompute(Scan)))"),
      counted("query 35", widthMinus().plan, exprGroups),
      shaped("query 18", byMinute().plan, "WideGroup(WideCompute(Scan))"),
      counted("query 18", byMinute().plan, minuteGroups),
      shaped("query 42", byMinuteStart().plan, "WideGroup(WideCompute(Scan))"),
      counted("query 42", byMinuteStart().plan, truncGroups),
    ).flatten

  private def bothPaths(): List[String] =
    List(
      both("query 0", countAll)(scan().count()),
      both("query 1", countAdv)(filtered().count()),
      both("query 2", mixedSums)(mixSums()),
      both("query 3", sumUser)(scan().sumLongColumn(2)),
      both("query 4", distinctUsers)(scan().countDistinctColumn(2)),
      both("query 5", distinctPhrases)(phrases().countDistinctColumn(3)),
      both("query 6", widthExtremum)(mixExtremum()),
      both("query 10", notEmpty)(notEmptyPhrase().count()),
      both("query 7", groupedMix)(groupedChecksum(grouped().collect())),
      both("query 8", distinctMix)(distinctChecksum(groupedDistinct().collect())),
      both("query 32", twoKeyMix)(twoKeyChecksum(groupedTwo().collect())),
      both("query 12", phraseMix)(phraseChecksum(groupedPhrase().collect())),
      both("query 11", twoStringMix)(phoneChecksum(groupedPhones().collect())),
      both("query 14", intStringMix)(intStringChecksum(groupedIntString().collect())),
      both("query 16", longStringMix)(longStringChecksum(groupedLongString().collect())),
      both("query 15", topUserMix)(topUserChecksum(topUsers().collect())),
      both("query 29", sumWidthPlus)(widthPlus().sumLongColumn(5)),
      both("query 20", likeUrlsCount)(likeUrls().count()),
      both("query 21", stringMinMix)(stringGroupChecksum(likeGroups().collect())),
      both("query 23", likeSortMix)(userChecksum(likeSorted().collect())),
      both("query 24", phraseFilterSortMix)(userPhraseChecksum(filteredPhrases().collect())),
      both("query 25", phraseSortMix)(userPhraseChecksum(sortedPhrases().collect())),
      both("query 27", urlLengthSum)(urlLengths().sumLongColumn(5)),
      both("query 35", exprGroupMix)(groupedChecksum(widthMinus().collect())),
      both("query 18", minuteMix)(groupedChecksum(byMinute().collect())),
      both("query 42", truncMix)(longGroupChecksum(byMinuteStart().collect())),
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

  private def distinct(
      name: String,
      plan: Plan[WideRow],
      ordinal: Int,
      expected: Long,
  ): Option[String] =
    WideDistinctCount.count(plan, ordinal) match
      case Some(value) if value == expected => None
      case Some(value) => Some(s"$name columnar distinct $value expected $expected")
      case None => Some(s"$name distinct did not lower")

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
      case Some(found) if Check.same(found, shape) => None
      case Some(found) => Some(s"$name shape $found expected $shape")
      case None => Some(s"$name did not lower")

  private def distinctBefore(left: WideRow, right: WideRow): Boolean =
    val leftCount = longAt(left, 1)
    val rightCount = longAt(right, 1)
    leftCount > rightCount || (leftCount == rightCount && intAt(left, 0) < intAt(right, 0))

  private def mixDistinct(acc: Long, row: WideRow): Long = cells(row) match
    case Vector(
          Cell.Int32(key),
          Cell.Int64(count),
          Cell.Int64(sum),
          Cell.Int32(min),
          Cell.Int32(max),
          Cell.Int64(distinct),
        ) =>
      (((((acc * 31L + key.toLong) * 31L + count) * 31L + sum) * 31L + min.toLong) * 31L +
        max.toLong) * 31L + distinct
    case other =>
      refuse(other.size)

  private def longGroupBefore(left: WideRow, right: WideRow): Boolean =
    val leftCount = longAt(left, 1)
    val rightCount = longAt(right, 1)
    leftCount > rightCount || (leftCount == rightCount && longAt(left, 0) < longAt(right, 0))

  private def groupBefore(left: WideRow, right: WideRow): Boolean =
    val leftCount = countOf(left)
    val rightCount = countOf(right)
    leftCount > rightCount || (leftCount == rightCount && keyOf(left) < keyOf(right))

  private def mixLong(acc: Long, row: WideRow): Long = cells(row) match
    case Vector(
          Cell.Int64(key),
          Cell.Int64(count),
          Cell.Int64(sum),
          Cell.Int64(min),
          Cell.Int64(max),
        ) =>
      (((((acc * 31L + key) * 31L + count) * 31L + sum) * 31L + min) * 31L + max)
    case other =>
      refuse(other.size)

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

  private def mixPhrase(acc: Long, row: WideRow): Long = cells(row) match
    case Vector(
          Cell.Utf8(key),
          Cell.Int64(count),
          Cell.Int64(sum),
          Cell.Int32(min),
          Cell.Int32(max),
        ) =>
      finish(foldText(acc, key), count, sum, min.toLong, max.toLong)
    case other => refuse(other.size)

  private def mixTexts(acc: Long, row: WideRow): Long = cells(row) match
    case Vector(
          Cell.Utf8(left),
          Cell.Utf8(right),
          Cell.Int64(count),
          Cell.Int64(sum),
          Cell.Int32(min),
          Cell.Int32(max),
        ) =>
      finish(foldText(foldText(acc, left), right), count, sum, min.toLong, max.toLong)
    case other => refuse(other.size)

  private def mixIntText(acc: Long, row: WideRow): Long = cells(row) match
    case Vector(
          Cell.Int32(key),
          Cell.Utf8(text),
          Cell.Int64(count),
          Cell.Int64(sum),
          Cell.Int32(min),
          Cell.Int32(max),
        ) =>
      finish(foldText(acc * 31L + key.toLong, text), count, sum, min.toLong, max.toLong)
    case other => refuse(other.size)

  private def mixLongText(acc: Long, row: WideRow): Long = cells(row) match
    case Vector(
          Cell.Int64(key),
          Cell.Utf8(text),
          Cell.Int64(count),
          Cell.Int64(sum),
          Cell.Int32(min),
          Cell.Int32(max),
        ) =>
      finish(foldText(acc * 31L + key, text), count, sum, min.toLong, max.toLong)
    case other => refuse(other.size)

  private def finish(acc: Long, count: Long, sum: Long, min: Long, max: Long): Long =
    (((acc * 31L + count) * 31L + sum) * 31L + min) * 31L + max

  private def foldText(acc: Long, text: String): Long =
    text.foldLeft(acc)((hash, ch) => hash * 31L + ch.toLong)

  private def stringGroupBefore(left: WideRow, right: WideRow): Boolean =
    val leftCount = longAt(left, 1)
    val rightCount = longAt(right, 1)
    leftCount > rightCount ||
    (leftCount == rightCount && textAt(left, 0).compareTo(textAt(right, 0)) < 0)

  private def mixStringGroup(acc: Long, row: WideRow): Long = cells(row) match
    case Vector(Cell.Utf8(key), Cell.Int64(count), Cell.Utf8(min), Cell.Utf8(max)) =>
      foldText(foldText(foldText(acc, key) * 31L + count, min), max)
    case other => refuse(other.size)

  private def mixUserPhrase(acc: Long, row: WideRow): Long =
    foldText(acc * 31L + longAt(row, 2), textAt(row, 3))

  private def textBefore(left: WideRow, right: WideRow): Boolean =
    val order = textAt(left, 0).compareTo(textAt(right, 0))
    order < 0 || (order == 0 && textAt(left, 1).compareTo(textAt(right, 1)) < 0)

  private def intTextBefore(left: WideRow, right: WideRow): Boolean =
    val leftKey = intAt(left, 0)
    val rightKey = intAt(right, 0)
    leftKey < rightKey || (leftKey == rightKey && textAt(left, 1).compareTo(textAt(right, 1)) < 0)

  private def longTextBefore(left: WideRow, right: WideRow): Boolean =
    val leftKey = longAt(left, 0)
    val rightKey = longAt(right, 0)
    leftKey < rightKey || (leftKey == rightKey && textAt(left, 1).compareTo(textAt(right, 1)) < 0)

  private def textAt(row: WideRow, ordinal: Int): String = Check.at(cells(row), ordinal) match
    case Cell.Utf8(value) => value
    case _ => refuse(cells(row).size)

  private def longAt(row: WideRow, ordinal: Int): Long = Check.at(cells(row), ordinal) match
    case Cell.Int64(value) => value
    case _ => refuse(cells(row).size)

  private def intAt(row: WideRow, ordinal: Int): Int = Check.at(cells(row), ordinal) match
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
