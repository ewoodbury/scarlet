package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.sql.{Sql, SqlResult}

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

  "a join" should "name the node" in {
    val thrown = the[IllegalArgumentException] thrownBy {
      val _ = Sql.run(
        "select count(*) from hits join hits on AdvEngineID = UserID",
        Map("hits" -> ClickBenchSlice.scan()),
      )
    }
    thrown.getMessage should include("Join")
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

  private def named(sql: String, node: String): Unit =
    val thrown = the[IllegalArgumentException] thrownBy {
      val _ = Sql.run(sql, Map("tiny" -> tiny()))
    }
    thrown.getMessage should include(node)

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
