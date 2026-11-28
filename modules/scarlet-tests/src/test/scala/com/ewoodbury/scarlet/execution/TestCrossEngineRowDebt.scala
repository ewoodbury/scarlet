package com.ewoodbury.scarlet.execution
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.support.Check.shouldBeEmpty

/**
 * The row-arm allowlist is debt. A listed shape that lowers has to leave the list. A scored shape
 * that still reports `row` has to be listed, with a reason.
 *
 * Plans are the harness plans. A few dozen rows, no actions, and no timing. A null value is
 * planted at index 1 (`index % 4096 == 1`), including in a 48-row source. Row 0 stays present so
 * the scan classifies. `filter-joinagg` with nulls is refused before a plan exists, so that cell
 * has no arm.
 */
class TestCrossEngineRowDebt extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val rows = 48

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 8192,
        threadPoolSize = 4,
        defaultShufflePartitions = 4,
        spillEnabled = true,
        traceLowering = false,
      ),
    )

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ()

  "the row-arm allowlist" should "name only shapes that still report row" in {
    val shapes = CrossEngineBench.rowArms.map(_.shape)
    shapes.distinct shouldBe shapes
    CrossEngineBench.rowArms.foreach { entry =>
      withClue(entry.shape) {
        entry.reason.trim.nonEmpty shouldBe true
      }
      CrossEngineBench.queryNames should contain(entry.shape)
    }
    CrossEngineBench.queryNames.foreach { name =>
      List(1, 2).foreach { filters =>
        List(false, true).foreach { nulls =>
          checkCell(name, filters, nulls)
        }
      }
    }
  }

  "a listed shape that lowers" should "have to leave the allowlist" in {
    val lowered = debt(listed = true, Some("FilterKey(Scan)"), "filter-count")
    lowered.map(_.contains("remove it")) shouldBe Some(true)
    debt(listed = true, None, "still-row").shouldBeEmpty
    debt(listed = false, Some("FilterKey(Scan)"), "columnar").shouldBeEmpty
    val missing = debt(listed = false, None, "filter-count")
    missing.map(_.contains("add it")) shouldBe Some(true)
  }

  "an allowlist entry" should "require a reason" in {
    val thrown = intercept[IllegalArgumentException] {
      val _ = CrossEngineBench.RowArm("filter-count", " ")
    }
    thrown.getMessage should include("without a reason")
  }

  "an unlisted row arm" should "fail the run" in {
    val listed = CrossEngineBench.rowArms.map(_.shape)
    CrossEngineBench.queryNames.filterNot(listed.contains).foreach { name =>
      val message =
        CrossEngineBench.rowArmRefusal(name, None).getOrElse(fail(s"$name was allowed"))
      message should include(name)
      message should include("arm=row")
    }
    CrossEngineBench.rowArms.foreach { entry =>
      CrossEngineBench.rowArmRefusal(entry.shape, None).shouldBeEmpty
    }
    CrossEngineBench.rowArmRefusal("filter-count", Some("FilterKey(Scan)")).shouldBeEmpty
    CrossEngineBench.unlistedRow("filter-count", Seq("filter-count")).shouldBeEmpty
    CrossEngineBench.unlistedRow("filter-count", Seq.empty[String]) shouldBe defined
  }

  "the human summary" should "print the arm of every shape" in {
    val published = CrossEngineBench.queryNames.zipWithIndex.map { case (name, index) =>
      val arm = if (index == 0) then "row" else s"Scan$index"
      (name, arm, "1.0", index.toLong, "ok")
    }
    val table = CrossEngineBench.summaryTable(published)
    table.linesIterator.next() shouldBe "| shape | arm | p50_ms | checksum | steady |"
    published.foreach { case (name, arm, p50, checksum, verdict) =>
      table should include(s"| $name | $arm | $p50 | $checksum | $verdict |")
    }
  }

  "a 48-row null source" should "plant one null value, at index 1" in {
    (0 until rows).filter(CrossEngineBench.nullValueAt) shouldBe Seq(1)
    CrossEngineBench.nullValueAt(0) shouldBe false
    CrossEngineBench.nullValueAt(4097) shouldBe true
  }

  private def checkCell(name: String, filters: Int, nulls: Boolean): Unit =
    name match
      case "filter-joinagg" if nulls => absentJoinAgg(filters)
      case _ =>
        val arm = CrossEngineBench.scoredArm(name, rows, filters, nulls)
        val listed = CrossEngineBench.rowArms.map(_.shape).contains(name)
        val where = s"$name filters=$filters nulls=$nulls"
        debt(listed, arm, where) match
          case Some(message) => fail(message)
          case None => ()

  /** No plan, so no arm. A later build of this cell has to join the matrix. */
  private def absentJoinAgg(filters: Int): Unit =
    val thrown = intercept[RuntimeException] {
      val _ = CrossEngineBench.scoredPlan("filter-joinagg", rows, filters, nulls = true)
    }
    thrown.getMessage should include("does not time null values")

  /** `Some` is a stale or missing allowlist entry. The live matrix must be `None`. */
  private def debt(listed: Boolean, arm: Option[String], where: String): Option[String] =
    (listed, arm) match
      case (true, Some(shape)) =>
        Some(s"$where reports $shape; remove it from the row-arm allowlist")
      case (false, None) =>
        Some(s"$where reports row; add it to the row-arm allowlist with a reason")
      case _ => None
