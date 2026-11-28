package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.sql.{Sql, SqlResult}
import com.ewoodbury.scarlet.support.Check

/** Three checksummed statements, plus a group, a boolean filter, and a named refusal. */
class TestSqlConvert extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get
  private val base = original.copy(
    columnarExecution = true,
    columnarBatchSize = 4,
    threadPoolSize = 2,
    defaultShufflePartitions = 2,
    spillEnabled = false,
  )

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(base)

  override def afterEach(): Unit =
    ScarletConf.set(original)
    ()

  "count" should "match query 0" in {
    val (result, shape) = value("select count(*) from hits", Map("hits" -> ClickBenchSlice.scan()))
    result shouldBe ClickBenchSlice.countAll
    shape shouldBe "Scan"
  }

  "a filter" should "match query 1" in {
    val (result, shape) = value(
      "select count(*) from hits where AdvEngineID <> 0",
      Map("hits" -> ClickBenchSlice.scan()),
    )
    result shouldBe ClickBenchSlice.countAdv
    shape shouldBe "WideFilter0(Scan)"
  }

  "an added literal" should "match query 29" in {
    val (result, shape) = value(
      "select sum(ResolutionWidth + 11) from hits",
      Map("hits" -> ClickBenchSlice.scan()),
    )
    result shouldBe ClickBenchSlice.sumWidthPlus
    shape shouldBe "WideCompute(WideCompute(WideCompute(Scan)))"
  }

  "a product" should "match query 6" in {
    val (result, shape) = value(
      """select sum(price * discount) from lineitem
        |where ship >= 100 and ship < 200 and discount >= 5 and discount <= 7
        |and quantity < 24""".stripMargin,
      Map("lineitem" -> lineitem()),
    )
    result shouldBe TpchSlice.revenue
    shape shouldBe "WideCompute(WideFilter2(WideFilter1(WideFilter1(WideFilter0(WideFilter0(Scan))))))"
  }

  "an empty phrase" should "match query 10" in {
    val (result, shape) = value(
      "select count(*) from hits where SearchPhrase <> ''",
      Map("hits" -> phraseHits()),
    )
    result shouldBe ClickBenchSlice.notEmpty
    shape shouldBe "WideFilter3(Scan)"
  }

  "a count by key" should "drop the other aggregates" in {
    val (result, shape) = grouped(
      """select AdvEngineID, count(*) from hits
        |where AdvEngineID <> 0 group by AdvEngineID""".stripMargin,
      Map("hits" -> ClickBenchSlice.scan()),
      countOnly,
    )
    result shouldBe ClickBenchSlice.countAdv
    shape shouldBe "WideProject(WideGroup(WideFilter0(Scan)))"
  }

  "a join" should "match the hand plan" in {
    val orders = DistCollection.ofColumns(Vector(WideInput.ints("id", Array(1, 2, 3))), 2)
    val lines = DistCollection.ofColumns(
      Vector(
        WideInput.ints("rid", Array(2, 3, 3)),
        WideInput.ints("amount", Array(10, 20, 30)),
      ),
      2,
    )
    val hand = orders.joinColumns(lines, 0, 0)
    val handCount = hand.count()
    val handShape = ColumnarPlanner.lower(hand.plan).map(_.shape).getOrElse("none")
    val (result, shape) = value(
      "select count(*) from orders join lines on orders.id = lines.rid",
      Map("orders" -> orders, "lines" -> lines),
    )
    result shouldBe handCount
    shape shouldBe handShape
  }

  "a sorted group" should "match query 15" in {
    val (result, shape) = grouped(
      """select UserID, count(*), sum(UserID), min(UserID), max(UserID)
        |from hits group by UserID
        |order by UserID desc
        |limit 10""".stripMargin,
      Map("hits" -> ClickBenchSlice.scan()),
      ClickBenchSlice.topUserChecksum,
    )
    result shouldBe ClickBenchSlice.topUserMix
    shape shouldBe "WideLimit(WideSort(WideGroup(Scan)))"
  }

  "count order" should "resolve count star" in {
    val sql =
      """select UserID, count(*) from hits group by UserID
        |order by count(*) desc limit 10""".stripMargin
    val (columnar, columnarShape) = listed(sql, columnar = true)
    val (rows, _) = listed(sql, columnar = false)
    columnar.length shouldBe 10
    rows.length shouldBe 10
    columnarShape shouldBe "WideLimit(WideSort(WideProject(WideGroup(Scan))))"
    countsOf(columnar) shouldBe List.fill(10)(1L)
    countsOf(rows) shouldBe List.fill(10)(1L)
  }

  "a descending limit" should "keep the largest ids" in {
    val (cells, shape) = listed(
      "select id from tiny order by id desc limit 2",
      columnar = true,
      Map("tiny" -> tiny()),
    )
    val (rows, _) = listed(
      "select id from tiny order by id desc limit 2",
      columnar = false,
      Map("tiny" -> tiny()),
    )
    cells shouldBe rows
    cells.map(_.cells) shouldBe List(Vector(Cell.Int32(4)), Vector(Cell.Int32(3)))
    shape shouldBe "WideLimit(WideSort(WideProject(Scan)))"
  }

  "a string group" should "match query 12" in {
    val (result, shape) = grouped(
      """select SearchPhrase, count(*), sum(AdvEngineID), min(AdvEngineID), max(AdvEngineID)
        |from hits group by SearchPhrase""".stripMargin,
      Map("hits" -> ClickBenchSlice.phrases()),
      ClickBenchSlice.phraseChecksum,
    )
    result shouldBe ClickBenchSlice.phraseMix
    shape shouldBe "WideGroup(Scan)"
  }

  "a group" should "match query 7" in {
    val (result, shape) = grouped(
      """select AdvEngineID, count(*), sum(AdvEngineID), min(AdvEngineID), max(AdvEngineID)
        |from hits where AdvEngineID <> 0 group by AdvEngineID""".stripMargin,
      Map("hits" -> ClickBenchSlice.scan()),
      ClickBenchSlice.groupedChecksum,
    )
    result shouldBe ClickBenchSlice.groupedMix
    shape shouldBe "WideGroup(WideFilter0(Scan))"
  }

  "or" should "keep either match" in {
    val (result, _) = value(
      "select count(*) from tiny where id = 1 or id = 3",
      Map("tiny" -> tiny()),
    )
    result shouldBe 2L
  }

  "not" should "negate one comparison" in {
    val (result, shape) = value(
      "select count(*) from tiny where not id = 2",
      Map("tiny" -> tiny()),
    )
    result shouldBe 3L
    shape shouldBe "WideFilter0(Scan)"
  }

  "divide" should "divide toward zero" in {
    val (result, _) = value("select sum(id / 2) from tiny", Map("tiny" -> tiny()))
    result shouldBe 4L
  }

  "a literal on the left" should "compare the column" in {
    val (atLeast, shape) = value(
      "select count(*) from tiny where 2 <= id",
      Map("tiny" -> tiny()),
    )
    atLeast shouldBe 3L
    shape shouldBe "WideFilter0(Scan)"
    val (atMost, _) = value(
      "select count(*) from tiny where 2 >= id",
      Map("tiny" -> tiny()),
    )
    atMost shouldBe 2L
  }

  "a union" should "name the node" in {
    val thrown = the[IllegalArgumentException] thrownBy {
      val _ = Sql.run(
        "select count(*) from tiny union select count(*) from tiny",
        Map("tiny" -> tiny()),
      )
    }
    thrown.getMessage should include("SetOperationList")
  }

  "an offset" should "name the node" in {
    named("select count(*) from tiny offset 1", "Offset")
  }

  "a fetch" should "name the node" in {
    named("select count(*) from tiny fetch first 1 row only", "Fetch")
  }

  "a top" should "name the node" in {
    named("select top 1 count(*) from tiny", "Top")
  }

  "a left join" should "name the node" in {
    val thrown = the[IllegalArgumentException] thrownBy {
      val _ = Sql.run(
        "select count(*) from tiny left join tiny on id = id",
        Map("tiny" -> tiny()),
      )
    }
    thrown.getMessage should include("Join")
  }

  "an ordered string" should "sort one string column" in {
    val sql =
      "select SearchPhrase from hits where SearchPhrase <> '' order by SearchPhrase limit 1"
    val tables = Map("hits" -> phraseHits())
    val (columnar, columnarShape) = listed(sql, columnar = true, tables)
    val (rows, _) = listed(sql, columnar = false, tables)
    columnarShape shouldBe "WideLimit(WideSort(WideProject(WideFilter3(Scan))))"
    columnar.map(_.cells) shouldBe rows.map(_.cells)
    columnar.headOption.map(_.cells) shouldBe Some(Vector(Cell.Utf8("p0")))
  }

  "an ordered expression" should "name the node" in {
    named("select id from tiny order by id + 1", "OrderByElement")
  }

  "two sort keys" should "name the second" in {
    named("select id from tiny order by id, id", "OrderByElement")
  }

  "a limit all" should "name the node" in {
    named("select id from tiny limit all", "Limit")
  }

  "a limit offset" should "name the node" in {
    named("select id from tiny limit 1, 2", "Limit")
  }

  "an ordered value" should "name the node" in {
    named("select count(*) from tiny order by id", "OrderByElement")
  }

  "a limited value" should "name the node" in {
    named("select count(*) from tiny limit 1", "Limit")
  }

  "a string order" should "count the rows below the literal" in {
    val tables = Map("hits" -> phraseHits())
    val (below, shape) = value("select count(*) from hits where SearchPhrase < 'a'", tables)
    below shouldBe 820L
    shape shouldBe "WideFilter3(Scan)"
    val (kept, _) = value("select count(*) from hits where not SearchPhrase < 'a'", tables)
    kept shouldBe 2457L
  }

  "not like" should "drop a null and a match" in {
    val table = DistCollection.ofColumns(
      Vector(
        WideInput.nullableStrings(
          "URL",
          Array("agoogle", "Google", "", "%xa", "x"),
          Array(true, true, true, true, false),
        ),
      ),
      2,
    )
    val tables = Map("hits" -> table)
    val (kept, shape) = value("select count(*) from hits where URL not like '%google%'", tables)
    kept shouldBe 3L
    shape shouldBe "WideFilter0(Scan)"
    val (matched, _) = value("select count(*) from hits where URL like '%a'", tables)
    matched shouldBe 1L
  }

  "or of like" should "name the node" in {
    val table = DistCollection.ofColumns(
      Vector(WideInput.strings("URL", Array("agoogle", "bing"))),
      2,
    )
    val thrown = the[IllegalArgumentException] thrownBy {
      val _ = Sql.run(
        "select count(*) from hits where URL like '%google%' or URL like '%bing%'",
        Map("hits" -> table),
      )
    }
    thrown.getMessage should include("LikeExpression")
  }

  "like" should "match query 20" in {
    val (result, shape) = value(
      "select count(*) from hits where URL like '%google%'",
      Map("hits" -> ClickBenchSlice.urls()),
    )
    result shouldBe ClickBenchSlice.likeUrlsCount
    shape shouldBe "WideFilter3(Scan)"
  }

  "length" should "match query 27" in {
    val (result, shape) = value(
      "select sum(length(URL)) from hits",
      Map("hits" -> ClickBenchSlice.urls()),
    )
    result shouldBe ClickBenchSlice.urlLengthSum
    shape shouldBe "WideCompute(WideCompute(Scan))"
  }

  "a string min" should "match query 21" in {
    val (rows, shape) = listed(
      """select SearchPhrase, count(*), min(URL)
        |from hits where URL like '%google%' and SearchPhrase <> ''
        |group by SearchPhrase""".stripMargin,
      columnar = true,
      Map("hits" -> ClickBenchSlice.phrasesAndUrls()),
    )
    shape shouldBe "WideProject(WideGroup(WideFilter3(WideFilter4(Scan))))"
    val (_, rowShape) = listed(
      """select SearchPhrase, count(*), min(URL)
        |from hits where URL like '%google%' and SearchPhrase <> ''
        |group by SearchPhrase""".stripMargin,
      columnar = false,
      Map("hits" -> ClickBenchSlice.phrasesAndUrls()),
    )
    rowShape shouldBe "none"
    val found = rows.collectFirst {
      case row if row.cells.headOption.contains(Cell.Utf8("p0")) => row.cells
    }
    found shouldBe Some(Vector(Cell.Utf8("p0"), Cell.Int64(578L), Cell.Utf8("agoogle")))
  }

  "a regex replace" should "name the node" in {
    val thrown = the[IllegalArgumentException] thrownBy {
      val _ = Sql.run(
        "select regexp_replace(SearchPhrase, 'a', 'b') from hits",
        Map("hits" -> phraseHits()),
      )
    }
    thrown.getMessage should include("Function")
  }

  "count distinct" should "match query 4" in {
    val (result, shape) = value(
      "select count(distinct UserID) from hits",
      Map("hits" -> ClickBenchSlice.scan()),
    )
    result shouldBe ClickBenchSlice.distinctUsers
    shape shouldBe "Scan"
  }

  "a distinct phrase" should "match query 5" in {
    val (result, shape) = value(
      "select count(distinct SearchPhrase) from hits",
      Map("hits" -> ClickBenchSlice.phrases()),
    )
    result shouldBe ClickBenchSlice.distinctPhrases
    shape shouldBe "Scan"
  }

  "a distinct group" should "match query 8" in {
    val sql = "select AdvEngineID, count(distinct ResolutionWidth) from hits group by AdvEngineID"
    val (columnar, shape) = listed(sql, columnar = true)
    val (rows, _) = listed(sql, columnar = false)
    shape shouldBe "WideProject(WideGroup(Scan))"
    columnar.length shouldBe 8
    rows.length shouldBe 8
    distinctOf(columnar, 0) shouldBe ClickBenchSlice.distinctWidths
    distinctOf(rows, 0) shouldBe ClickBenchSlice.distinctWidths
  }

  "count and distinct" should "keep both columns" in {
    val sql =
      """select AdvEngineID, count(*), count(distinct ResolutionWidth)
        |from hits group by AdvEngineID""".stripMargin
    val (columnar, shape) = listed(sql, columnar = true)
    val (rows, _) = listed(sql, columnar = false)
    shape shouldBe "WideProject(WideGroup(Scan))"
    countAndDistinct(columnar, 0) shouldBe ((512L, ClickBenchSlice.distinctWidths))
    countAndDistinct(rows, 0) shouldBe ((512L, ClickBenchSlice.distinctWidths))
  }

  "a string key" should "count distinct values of its measure" in {
    val sql =
      """select SearchPhrase, sum(AdvEngineID), count(distinct AdvEngineID)
        |from hits group by SearchPhrase""".stripMargin
    val (columnar, shape) = listed(sql, columnar = true, Map("hits" -> ClickBenchSlice.phrases()))
    val (rows, _) = listed(sql, columnar = false, Map("hits" -> ClickBenchSlice.phrases()))
    shape shouldBe "WideProject(WideGroup(Scan))"
    phraseDistinct(columnar, "p0") shouldBe ((2048L, 2L))
    phraseDistinct(rows, "p0") shouldBe ((2048L, 2L))
  }

  "distinct of an expression" should "name the node" in {
    named("select count(distinct id + 1) from tiny", "Addition")
  }

  "sum distinct" should "name the node" in {
    named("select sum(distinct id) from tiny", "Function")
  }

  "count distinct star" should "name the node" in {
    named("select count(distinct *) from tiny", "Function")
  }

  "two distinct counts" should "be refused" in {
    val thrown = the[IllegalArgumentException] thrownBy {
      val _ = Sql.run(
        """select AdvEngineID, count(distinct ResolutionWidth), count(distinct UserID)
          |from hits group by AdvEngineID""".stripMargin,
        Map("hits" -> ClickBenchSlice.scan()),
      )
    }
    thrown.getMessage should include("a group has one distinct count")
  }

  "a float distinct" should "name the kind" in {
    val table = DistCollection.ofColumns(Vector(WideInput.doubles("w", Array(1.0, 2.0))), 2)
    val thrown = the[IllegalArgumentException] thrownBy {
      val _ = Sql.run("select count(distinct w) from t", Map("t" -> table))
    }
    thrown.getMessage should include("distinct of Float64 is not supported")
  }

  "a bool distinct" should "name the kind" in {
    val table = DistCollection.ofColumns(Vector(WideInput.bools("b", Array(true, false))), 2)
    val thrown = the[IllegalArgumentException] thrownBy {
      val _ = Sql.run("select count(distinct b) from t", Map("t" -> table))
    }
    thrown.getMessage should include("distinct of Bool is not supported")
  }

  "a decimal distinct" should "name the kind" in {
    val table = DistCollection.ofColumns(
      Vector(WideInput.decimals("d", 4, 0, Array(1L, 2L))),
      2,
    )
    val thrown = the[IllegalArgumentException] thrownBy {
      val _ = Sql.run("select count(distinct d) from t", Map("t" -> table))
    }
    thrown.getMessage should include("distinct of Decimal")
  }

  "between" should "name the node" in {
    val thrown = the[IllegalArgumentException] thrownBy {
      val _ = Sql.run(
        "select count(*) from hits where AdvEngineID between 1 and 3",
        Map("hits" -> ClickBenchSlice.scan()),
      )
    }
    thrown.getMessage should include("Between")
  }

  private def value(sql: String, tables: Map[String, DistCollection[WideRow]]): (Long, String) =
    val columnar = once(sql, tables, columnar = true)
    val rows = once(sql, tables, columnar = false)
    columnar._1 shouldBe rows._1
    columnar

  private def once(
      sql: String,
      tables: Map[String, DistCollection[WideRow]],
      columnar: Boolean,
  ): (Long, String) =
    ScarletConf.set(base.copy(columnarExecution = columnar))
    Sql.run(sql, tables) match
      case SqlResult.Value(result, plan) =>
        val shape = ColumnarPlanner.lower(plan).map(_.shape).getOrElse("none")
        if columnar then shape should not be "none"
        (result, shape)
      case SqlResult.Rows(_) => fail(s"expected a value: $sql")

  private def grouped(
      sql: String,
      tables: Map[String, DistCollection[WideRow]],
      mix: Iterable[WideRow] => Long,
  ): (Long, String) =
    def read(columnar: Boolean): (Long, String) =
      ScarletConf.set(base.copy(columnarExecution = columnar))
      Sql.run(sql, tables) match
        case SqlResult.Rows(rows) =>
          val shape = ColumnarPlanner.lower(rows.plan).map(_.shape).getOrElse("none")
          if columnar then shape should not be "none"
          (mix(rows.collect()), shape)
        case SqlResult.Value(_, _) => fail(s"expected rows: $sql")
    val columnar = read(true)
    val rows = read(false)
    columnar._1 shouldBe rows._1
    columnar

  private def listed(
      sql: String,
      columnar: Boolean,
      tables: Map[String, DistCollection[WideRow]] = Map("hits" -> ClickBenchSlice.scan()),
  ): (List[WideRow], String) =
    ScarletConf.set(base.copy(columnarExecution = columnar))
    Sql.run(sql, tables) match
      case SqlResult.Rows(rows) =>
        val shape = ColumnarPlanner.lower(rows.plan).map(_.shape).getOrElse("none")
        if columnar then shape should not be "none"
        (rows.collect().toList, shape)
      case SqlResult.Value(_, _) => fail(s"expected rows: $sql")

  private def distinctOf(rows: List[WideRow], key: Int): Long =
    rows
      .collectFirst {
        case row if row.cells.headOption.contains(Cell.Int32(key)) =>
          row.cells.lift(1) match
            case Some(Cell.Int64(distinct)) => distinct
            case _ => fail(s"missing distinct for $key")
      }
      .getOrElse(fail(s"missing group $key"))

  private def countAndDistinct(rows: List[WideRow], key: Int): (Long, Long) =
    rows
      .collectFirst {
        case row if row.cells.headOption.contains(Cell.Int32(key)) =>
          (row.cells.lift(1), row.cells.lift(2)) match
            case (Some(Cell.Int64(count)), Some(Cell.Int64(distinct))) => (count, distinct)
            case _ => fail(s"missing count and distinct for $key")
      }
      .getOrElse(fail(s"missing group $key"))

  private def phraseDistinct(rows: List[WideRow], key: String): (Long, Long) =
    rows
      .collectFirst {
        case row if row.cells.headOption.contains(Cell.Utf8(key)) =>
          (row.cells.lift(1), row.cells.lift(2)) match
            case (Some(Cell.Int64(sum)), Some(Cell.Int64(distinct))) => (sum, distinct)
            case _ => fail(s"missing sum and distinct for $key")
      }
      .getOrElse(fail(s"missing group $key"))

  private def countsOf(rows: List[WideRow]): List[Long] =
    rows.map { row =>
      row.cells.lift(1) match
        case Some(Cell.Int64(count)) => count
        case _ => fail("a grouped count is the second cell")
    }

  private def named(sql: String, node: String): Unit =
    val thrown = the[IllegalArgumentException] thrownBy {
      val _ = Sql.run(sql, Map("tiny" -> tiny()))
    }
    thrown.getMessage should include(node)

  private def countOnly(rows: Iterable[WideRow]): Long =
    rows.foldLeft(0L) { (total, row) =>
      row.cells.lift(1) match
        case Some(Cell.Int64(count)) => total + count
        case _ => fail("a grouped count is the second cell")
    }

  private def phraseHits(): DistCollection[WideRow] =
    val rows = ClickBenchSlice.rowCount
    val phrase = WideInput.nullableStrings(
      "SearchPhrase",
      Array.tabulate(rows) { index =>
        if Check.same(index % 5, 0) then "" else "p" + (index % 7).toString
      },
      Array.tabulate(rows)(index => !Check.same(index % 5, 1)),
    )
    DistCollection.ofColumns(
      ClickBenchSlice.scanInputs() :+ phrase,
      ClickBenchSlice.partitions,
    )

  private def lineitem(): DistCollection[WideRow] =
    val rows = TpchSlice.lineitemRows
    DistCollection.ofColumns(
      Vector(
        WideInput.longs("ship", Array.tabulate(rows)(index => 50L + (index % 200).toLong)),
        WideInput.longs("discount", Array.tabulate(rows)(index => (index % 11).toLong)),
        WideInput.longs("quantity", Array.tabulate(rows)(index => (index % 40).toLong)),
        WideInput.longs("price", Array.tabulate(rows)(index => ((index * 17) % 1000).toLong)),
      ),
      TpchSlice.partitions,
    )

  private def tiny(): DistCollection[WideRow] =
    DistCollection.ofColumns(Vector(WideInput.ints("id", Array(1, 2, 3, 4))), 2)
