package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnBatch, ColumnPipeline, Int32Column}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

import ColumnPipeline.JoinInput

/**
 * Owned int-pair filters fold into the encode. The folded join and the survivor-index join publish
 * the same rows in the same order. A boxed source stays on the index.
 */
class TestIntPairFilterJoin extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 3,
        defaultShufflePartitions = 4,
        threadPoolSize = 2,
        spillEnabled = false,
      ),
    )

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ScarletRuntime.get.shuffle.clear()
    ()

  "an owned filter join" should "match the survivor index row for row" in {
    val keys = Array(1, 3, 1, 2, 4, 0, 5, 1)
    val values = Array(5, 6, 7, -1, 8, 9, -2, 4)
    val probeKeys = Array(1, 9, 4, 1)
    val probeValues = Array(10, 11, 12, 13)
    val left = filtered(keys, values)
    val right = DistCollection.ofIntPairs(probeKeys, probeValues, 2)
    val joined = left.join(right)
    val (plan, op, leftPlan, leftOp, rightPlan, rightOp) = lowered(joined.plan)
    val ordinals = ColumnUse.ordinals(plan, op)

    IntPairFilterEncode.tryBatches(leftPlan, leftOp, ordinals).isDefined shouldBe true
    IntPairFilterEncode.tryBatches(rightPlan, rightOp, ordinals).isEmpty shouldBe true

    val folded = JoinArms.inner(leftPlan, rightPlan, leftOp, rightOp, None, false, ordinals)
    val indexed = legacy(leftPlan, rightPlan, leftOp, rightOp, ordinals)
    val foldedRows = triples(folded)
    val indexedRows = triples(indexed)

    foldedRows shouldBe indexedRows
    foldedRows.map(_._2) should contain allOf (5, 4)
    foldedRows.exists { case (_, _, probe) => probe == 11 } shouldBe false
    foldedRows.count { case (key, _, _) => key == 1 } shouldBe 6
    foldedRows.count { case (key, _, _) => key == 4 } shouldBe 1
    JoinArms.count(leftPlan, rightPlan, leftOp, rightOp, None, false, ordinals) shouldBe
      indexedRows.length.toLong
    joined.collect().toVector shouldBe foldedRows.map { case (key, build, probe) =>
      (key, (build, probe))
    }
  }

  it should "keep an empty side, a miss, and duplicate build keys" in {
    val present = filtered(Array(1, 1, 2), Array(10, 11, 20))
    val empty = filtered(Array.empty[Int], Array.empty[Int])
    val miss = DistCollection.ofIntPairs(Array(8, 9), Array(1, 2), 2)
    val duplicated = DistCollection.ofIntPairs(Array(1, 1), Array(100, 101), 1)

    agree(empty.join(present))
    agree(present.join(empty))
    agree(present.join(miss))
    agree(present.join(duplicated))
    empty.join(present).count() shouldBe 0L
    present.join(empty).count() shouldBe 0L
    present.join(miss).count() shouldBe 0L
    present.join(duplicated).count() shouldBe 4L
  }

  it should "leave a boxed source on the survivor index" in {
    val left = DistCollection(Seq((1, 5), (3, 6), (1, 4)), 2)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val right = DistCollection.ofIntPairs(Array(1, 1), Array(10, 11), 1)
    val joined = left.join(right)
    val (plan, op, leftPlan, leftOp, rightPlan, rightOp) = lowered(joined.plan)
    val ordinals = ColumnUse.ordinals(plan, op)

    IntPairFilterEncode.tryBatches(leftPlan, leftOp, ordinals).isEmpty shouldBe true
    val folded = triples(
      JoinArms.inner(leftPlan, rightPlan, leftOp, rightOp, None, true, ordinals),
    )
    folded shouldBe triples(legacy(leftPlan, rightPlan, leftOp, rightOp, ordinals))
  }

  private def agree(joined: DistCollection[(Int, (Int, Int))]): Unit =
    val (plan, op, leftPlan, leftOp, rightPlan, rightOp) = lowered(joined.plan)
    val ordinals = ColumnUse.ordinals(plan, op)
    val folded = triples(
      JoinArms.inner(leftPlan, rightPlan, leftOp, rightOp, None, true, ordinals),
    )
    folded shouldBe triples(legacy(leftPlan, rightPlan, leftOp, rightOp, ordinals))
    joined.count() shouldBe folded.length.toLong

  private def filtered(keys: Array[Int], values: Array[Int]): DistCollection[(Int, Int)] =
    DistCollection
      .ofIntPairs(keys, values, 2)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)

  private def lowered(
      plan: Plan[_],
  ): (Plan[_], PhysicalOp, Plan[_], PhysicalOp, Plan[_], PhysicalOp) =
    val op = ColumnarPlanner.lower(plan).getOrElse(fail("join did not lower"))
    (plan, op) match
      case (
            join @ Plan.JoinOp(left, right, _),
            PhysicalOp.HashJoin(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)),
          ) =>
        (join, op, left, leftOp, right, rightOp)
      case _ => fail(s"join lowered to ${op.getClass.getName}")

  /** The index path [[JoinArms]] used before the owned copy. */
  private def legacy(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    val leftSelected = FilterChain.selected(left, leftOp, None, true, ordinals)
    val rightSelected = FilterChain.selected(right, rightOp, None, true, ordinals)
    (leftSelected, rightSelected) match
      case (None, None) =>
        ColumnPipeline.innerJoin(
          pairs(left, leftOp, ordinals),
          pairs(right, rightOp, ordinals),
          width,
          workers,
          batchSize,
        )
      case _ =>
        ColumnPipeline.innerJoin(
          legacyInput(left, leftOp, leftSelected, ordinals),
          legacyInput(right, rightOp, rightSelected, ordinals),
          width,
          workers,
        )

  private def legacyInput(
      plan: Plan[_],
      op: PhysicalOp,
      selected: Option[Vector[com.ewoodbury.scarlet.columnar.Selected]],
      ordinals: Set[Int],
  ): JoinInput =
    selected match
      case Some(parts) => JoinInput.Selected(parts)
      case None => JoinInput.Dense(pairs(plan, op, ordinals))

  private def pairs(plan: Plan[_], op: PhysicalOp, ordinals: Set[Int]): Vector[ColumnBatch] =
    ColumnarInterpreter.pairsOf(
      ColumnarInterpreter.interpretAt(plan, op, None, true, ordinals),
    )

  private def triples(batches: Vector[ColumnBatch]): Vector[(Int, Int, Int)] =
    batches.flatMap { batch =>
      val keys = ints(batch, 0)
      val build = ints(batch, 1)
      val probe = ints(batch, 2)
      Vector.tabulate(batch.length)(row => (keys(row), build(row), probe(row)))
    }

  private def ints(batch: ColumnBatch, ordinal: Int): Array[Int] =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column.values
      case other => fail(s"column $ordinal is $other")
