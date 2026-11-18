package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * The row-arm allowlist is debt. A listed shape that lowers has to leave the list. A scored
 * shape that still reports `row` has to be listed, with a reason.
 *
 * Plans are the harness plans. A few dozen rows, no actions, and no timing. A null value sits
 * at index 1, the same stride as [[CrossEngineBench]]. `filter-joinagg` with nulls is refused
 * before a plan exists, so it has no arm.
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
          name match
            case "filter-joinagg" if nulls => ()
            case _ => check(name, filters, nulls)
        }
      }
    }
  }

  "an unlisted row arm" should "fail the run" in {
    val message =
      CrossEngineBench.rowArmRefusal("filter-count", None).getOrElse(fail("missing refusal"))
    message should include("filter-count")
    message should include("arm=row")
    CrossEngineBench.rowArmRefusal("filter-count", Some("FilterKey(Scan)")) shouldBe None
    CrossEngineBench.unlistedRow("filter-count", Seq("filter-count")) shouldBe None
    CrossEngineBench.unlistedRow("filter-count", Seq.empty[String]) shouldBe defined
  }

  private def check(name: String, filters: Int, nulls: Boolean): Unit =
    val arm = CrossEngineBench.scoredArm(name, rows, filters, nulls)
    val listed = CrossEngineBench.rowArms.map(_.shape).contains(name)
    val where = s"$name filters=$filters nulls=$nulls"
    (listed, arm) match
      case (true, Some(shape)) =>
        fail(s"$where reports $shape; remove it from the row-arm allowlist")
      case (false, None) =>
        fail(s"$where reports row; add it to the row-arm allowlist with a reason")
      case _ => ()
