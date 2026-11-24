package com.ewoodbury.scarlet.execution

import scala.io.Source
import scala.util.Using

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.Plan
import com.ewoodbury.scarlet.execution.schema.{Describe, KernelChoice}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Admission ledger for [[TestColumnarContract.acceptedCases]].
 *
 * Each accepted label is recorded with the root [[KernelChoice]] from [[Describe.describe]]. The
 * golden file is `admission-ledger.txt`, one label and one tag separated by a tab. Additions,
 * removals, and a changed root fail unless that label is listed in `admission-allowlist.txt`. A
 * missing allowlist is empty, so no admission change passes.
 *
 * An allowlist row is a label, a tag, and an optional fused entry: `byKeyOwned`,
 * `byKeyOwnedMapped`, or `plain`. A row with no entry is allowed only when that kernel has no
 * `byKeyOwned` or `byKeyOwnedMapped` and the plain `byKey` did not run. A wrong entry fails.
 */
class TestAdmissionLedger extends AnyFlatSpec with BeforeAndAfterEach with ColumnarContractFixture:

  private val Ledger = "admission-ledger.txt"
  private val Allowlist = "admission-allowlist.txt"
  private val OwnedEntry = "byKeyOwned"
  private val OwnedMappedEntry = "byKeyOwnedMapped"
  private val PlainEntry = "plain"

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
    val plans = cases.map(item => item.label -> item.plan).toMap
    val golden = lines(resource(Ledger).getOrElse(""))
    val allow = allowed(resource(Allowlist).getOrElse(""))
    val allowByLabel = allow.map(row => row.label -> row).toMap
    val added = current.keySet.diff(golden.keySet)
    val removed = golden.keySet.diff(current.keySet)
    val retagged = current.keySet.intersect(golden.keySet).filterNot { label =>
      Ordering[String].equiv(current(label), golden(label))
    }
    val report = diff(added, removed, retagged, current, golden, allowByLabel)
    if added.nonEmpty || removed.nonEmpty || retagged.nonEmpty then println(report)
    val unexplained = (added ++ removed ++ retagged).diff(allowByLabel.keySet)
    assert(unexplained.isEmpty, report)
    assert(removed.isEmpty, report)
    assert(retagged.isEmpty, report)
    assert(
      allowByLabel.keySet.diff(added).isEmpty && added.diff(allowByLabel.keySet).isEmpty,
      report,
    )
    allow.foreach { row =>
      assert(
        Ordering[String].equiv(current(row.label), row.tag),
        s"${row.label} tag ${current(row.label)}, allowlist ${row.tag}",
      )
      val seen = fused(plans(row.label))
      assert(
        Ordering[String].equiv(seen, row.entry),
        s"${row.label} fused entry [$seen], allowlist [${row.entry}]",
      )
      if (row.entry.isEmpty) then
        assert(
          !Ordering[String].equiv(row.tag, KernelChoice.CountSum.tag),
          s"${row.label} has no fused entry, and CountSum still has byKeyOwned",
        )
    }
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

  private final case class Allowed(label: String, tag: String, entry: String)

  private def allowed(text: String): Seq[Allowed] =
    val parsed = text
      .replace("\r\n", "\n")
      .split("\n")
      .iterator
      .map(_.trim)
      .filter(line => line.nonEmpty && !line.startsWith("#"))
      .map(allowRow)
      .toSeq
    val duplicate = parsed.map(_.label).diff(parsed.map(_.label).distinct)
    assert(duplicate.isEmpty, s"duplicate allowlist labels: ${duplicate.distinct.mkString(", ")}")
    parsed

  private def allowRow(line: String): Allowed =
    val fields = line.split("\t", -1).toList
    val row = fields match
      case label :: tag :: Nil => Allowed(label, tag, "")
      case label :: tag :: entry :: Nil => Allowed(label, tag, entry)
      case _ => fail(s"expected a label, a tag, and an optional fused entry: [$line]")
    assert(
      row.label.nonEmpty && row.tag.nonEmpty,
      s"allowlist row needs a label and a tag: [$line]",
    )
    val knownEntry =
      row.entry.isEmpty || Ordering[String].equiv(row.entry, OwnedEntry) ||
        Ordering[String].equiv(row.entry, OwnedMappedEntry) ||
        Ordering[String].equiv(row.entry, PlainEntry)
    assert(
      knownEntry,
      s"fused entry must be $OwnedEntry, $OwnedMappedEntry, $PlainEntry, or absent: [$line]",
    )
    row

  private def fused(plan: Plan[_]): String =
    CountSum.resetByKeyForTest()
    CountSum.resetOwnedForTest()
    HashCount.resetByKeyForTest()
    HashReduce.resetByKeyForTest()
    LongHashCount.resetByKeyForTest()
    LongHashReduce.resetByKeyForTest()
    StringHashCount.resetByKeyForTest()
    StringHashReduce.resetByKeyForTest()
    ScarletRuntime.get.shuffle.clear()
    assert(ColumnarPlanner.tryRun(plan).isDefined, "columnar path did not publish a batch")
    val owned = List(
      Option.when(CountSum.byKeyOwnedCallsForTest > 0)(OwnedEntry),
      Option.when(CountSum.byKeyOwnedMappedCallsForTest > 0)(OwnedMappedEntry),
    ).flatten
    val plain = plainCalls
    assert(
      owned.size + plain.size <= 1,
      s"more than one fused entry ran: ${(owned ++ plain).mkString(", ")}",
    )
    if (plain.nonEmpty) then PlainEntry else owned.headOption.getOrElse("")

  private def plainCalls: List[String] =
    List(
      "CountSum.byKey" -> CountSum.byKeyCallsForTest,
      "HashCount.byKey" -> HashCount.byKeyCallsForTest,
      "HashReduce.byKey" -> HashReduce.byKeyCallsForTest,
      "LongHashCount.byKey" -> LongHashCount.byKeyCallsForTest,
      "LongHashReduce.byKey" -> LongHashReduce.byKeyCallsForTest,
      "StringHashCount.byKey" -> StringHashCount.byKeyCallsForTest,
      "StringHashReduce.byKey" -> StringHashReduce.byKeyCallsForTest,
    ).collect { case (name, count) if count > 0 => name }

  private def diff(
      added: Set[String],
      removed: Set[String],
      retagged: Set[String],
      current: Map[String, String],
      golden: Map[String, String],
      allow: Map[String, Allowed],
  ): String =
    def block(title: String, labels: Set[String])(render: String => String): String =
      if labels.isEmpty then ""
      else labels.toSeq.sorted.map(render).mkString(s"$title\n", "\n", "\n")

    def addition(label: String): String = allow.get(label) match
      case Some(row) if row.entry.isEmpty => s"${row.label}\t${row.tag}"
      case Some(row) => s"${row.label}\t${row.tag}\t${row.entry}"
      case None => s"$label\t${current(label)}"

    block("additions:", added)(addition) +
      block("removals:", removed)(label => s"$label\t${golden(label)}") +
      block("root changes:", retagged)(label => s"$label\t${golden(label)}\t${current(label)}")
