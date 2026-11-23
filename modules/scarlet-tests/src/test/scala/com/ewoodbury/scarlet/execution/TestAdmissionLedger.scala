package com.ewoodbury.scarlet.execution

import scala.io.Source
import scala.util.Using

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.Plan
import com.ewoodbury.scarlet.execution.schema.{Describe, KernelChoice}

/**
 * Admission ledger for [[TestColumnarContract.acceptedCases]].
 *
 * Each accepted label is recorded with the root [[KernelChoice]] from [[Describe.describe]]. The
 * golden file is `admission-ledger.txt`, one label and one tag separated by a tab. Additions,
 * removals, and a changed root fail unless that label is listed in `admission-allowlist.txt`. A
 * missing allowlist is empty, so no admission change passes.
 */
class TestAdmissionLedger extends AnyFlatSpec with BeforeAndAfterEach with ColumnarContractFixture:

  private val Ledger = "admission-ledger.txt"
  private val Allowlist = "admission-allowlist.txt"

  override def beforeEach(): Unit =
    super.beforeEach()
    installContractConf()

  override def afterEach(): Unit =
    restoreContractConf()
    ()

  "the admission ledger" should "record every accepted label and its root tag" in {
    val cases = new TestColumnarContract().acceptedCases
    val rows = cases.map { item =>
      Describe.describe(item.plan) match
        case Some(tagged) => item.label -> tagged.choice.tag
        case None => fail(s"${item.label} has no kernel tag")
    }
    val duplicate = rows.map(_._1).diff(rows.map(_._1).distinct)
    assert(duplicate.isEmpty, s"duplicate accepted labels: ${duplicate.distinct.mkString(", ")}")
    rows.foreach { case (label, _) =>
      assert(
        !label.contains('\t') && !label.contains('\n'),
        s"label needs one ledger field: $label",
      )
    }

    val current = rows.toMap
    val golden = lines(resource(Ledger).getOrElse(""))
    val allow = names(resource(Allowlist).getOrElse(""))
    val added = current.keySet.diff(golden.keySet)
    val removed = golden.keySet.diff(current.keySet)
    val retagged = current.keySet.intersect(golden.keySet).filterNot { label =>
      Ordering[String].equiv(current(label), golden(label))
    }
    val report = diff(added, removed, retagged, current, golden, allow)
    if added.nonEmpty || removed.nonEmpty || retagged.nonEmpty then println(report)
    val unexplained = (added ++ removed ++ retagged).diff(allow)
    assert(unexplained.isEmpty, report)
  }

  it should "tag the roots that are not the case name, and refuse without a tag" in {
    val joined = Plan.JoinOp(pairSource, rightPairSource, None)
    val projected = Plan.MapOp(
      joined,
      (row: (Int, (Int, Int))) => (row._1, row._2._1.toLong),
    )
    val summed = Plan.ReduceByKeyOp(projected, (left: Long, right: Long) => left + right)
    Describe.describe(Plan.CountByKeyOp(joined)).map(_.choice) shouldBe Some(
      KernelChoice.CountThroughProbe,
    )
    Describe.describe(summed).map(_.choice) shouldBe Some(KernelChoice.IntLongProjectSum)
    Describe.describe(DistCollection(joined).sumJoinLeft.plan).map(_.choice) shouldBe Some(
      KernelChoice.JoinProbeSum,
    )
    Describe.describe(Plan.SortByIntValueDescOp(pairSource)).map(_.choice) shouldBe Some(
      KernelChoice.SortByIntValueDesc,
    )
    val scan = Describe.describe(intSource)
    scan.map(_.root) shouldBe ColumnarPlanner.lower(intSource)
    scan.map(_.choice) shouldBe Some(KernelChoice.Scan)
    Describe
      .describe(
        Plan.FlatMapOp(intSource, (value: Int) => List(value)),
      )
      .isEmpty shouldBe true
  }

  private def resource(name: String): Option[String] =
    Option(getClass.getClassLoader.getResourceAsStream(name)).map { stream =>
      Using.resource(Source.fromInputStream(stream, "UTF-8"))(_.mkString)
    }

  private def lines(text: String): Map[String, String] =
    val parsed = text
      .replace("\r\n", "\n")
      .split("\n")
      .iterator
      .filter(_.nonEmpty)
      .map { line =>
        val tab = line.indexOf('\t')
        assert(
          tab > 0 && line.indexOf('\t', tab + 1) < 0,
          s"expected a label and a tag separated by one tab: [$line]",
        )
        line.substring(0, tab) -> line.substring(tab + 1)
      }
      .toSeq
    val duplicate = parsed.map(_._1).diff(parsed.map(_._1).distinct)
    assert(duplicate.isEmpty, s"duplicate ledger labels: ${duplicate.distinct.mkString(", ")}")
    parsed.toMap

  private def names(text: String): Set[String] =
    text
      .replace("\r\n", "\n")
      .split("\n")
      .iterator
      .map(_.trim)
      .filter(line => line.nonEmpty && !line.startsWith("#"))
      .toSet

  private def diff(
      added: Set[String],
      removed: Set[String],
      retagged: Set[String],
      current: Map[String, String],
      golden: Map[String, String],
      allow: Set[String],
  ): String =
    def block(title: String, labels: Set[String])(render: String => String): String =
      if labels.isEmpty then ""
      else labels.toSeq.sorted.map(render).mkString(s"$title\n", "\n", "\n")

    val body =
      block("additions:", added)(label => s"$label\t${current(label)}") +
        block("removals:", removed)(label => s"$label\t${golden(label)}") +
        block("root changes:", retagged)(label => s"$label\t${golden(label)}\t${current(label)}")
    val listed = if allow.isEmpty then "allowlist is empty" else allow.toSeq.sorted.mkString(", ")
    s"$body$listed\n"
