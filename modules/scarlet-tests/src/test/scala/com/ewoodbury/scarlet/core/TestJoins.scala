package com.ewoodbury.scarlet.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.execution.{ColumnarPlanner, LeftOuterJoinWideOp, StageBuilder, WideOpKind}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

class TestJoins extends AnyFlatSpec with Matchers {

  private val toDistCollection: [T] => (seq: Seq[T]) => DistCollection[T] =
    [T] => (seq: Seq[T]) => DistCollection(Plan.Source(Seq(Partition(seq))))

  it should "read the explicit left/right join inputs, not unrelated prior shuffles" in {
    // This test is for a specific bug where joins were using recent shuffles as inputs,
    // rather than the explicit left/right inputs.

    // Arrange
    ScarletRuntime.get.shuffle.clear()

    val left  = toDistCollection(Seq("a" -> 1))
    val right = toDistCollection(Seq("a" -> 2))

    // Create an unrelated branch that performs a shuffle before the join executes
    val unrelatedShuffled: DistCollection[(String, (Int, Int))] =
      toDistCollection(Seq("u" -> 999))
        .groupByKey
        .map { case (k, vs) => (k, (vs.headOption.getOrElse(0), -1)) }

    // Build a plan where the unrelated branch is on the left of a union, so its shuffle stages
    // are constructed and (by current build order) scheduled prior to the join stage.
    val combined: DistCollection[(String, (Int, Int))] =
      unrelatedShuffled.union(left.join(right))

    // Act
    val allResults = combined.collect().toSeq

    // Assert: The join result for key "a" should be present. With the current heuristic that
    // scans prior shuffles, the join may incorrectly read the unrelated shuffle as one side and
    // thus drop the "a" result, making this assertion fail.
    val joinOnly = allResults.filter(_._1 === "a")
    joinOnly shouldBe Seq("a" -> (1, 2))
  }

  it should "perform inner join as cartesian product for matching keys" in {
    // Arrange
    ScarletRuntime.get.shuffle.clear()

    val left  = toDistCollection(Seq("a" -> 1, "a" -> 3, "b" -> 7))
    val right = toDistCollection(Seq("a" -> 2, "a" -> 4, "c" -> 9))

    // Act
    val results = left.join(right).collect().toSeq

    // Assert
    val expectedForA = Seq(
      "a" -> (1, 2),
      "a" -> (1, 4),
      "a" -> (3, 2),
      "a" -> (3, 4),
    )

    val forA = results.filter(_._1 === "a")
    forA should contain theSameElementsAs expectedForA

    // Only keys present in both sides should appear (inner join semantics)
    results.map(_._1).toSet shouldBe Set("a")
  }

  it should "handle multi-key joins with cartesian product per key" in {
    // Arrange
    ScarletRuntime.get.shuffle.clear()

    val left  = toDistCollection(Seq("a" -> 1, "a" -> 3, "b" -> 5))
    val right = toDistCollection(Seq("a" -> 2, "a" -> 4, "b" -> 6, "b" -> 7))

    // Act
    val results = left.join(right).collect().toSeq

    // Assert per key
    val expectedA = Seq(
      "a" -> (1, 2),
      "a" -> (1, 4),
      "a" -> (3, 2),
      "a" -> (3, 4),
    )
    val expectedB = Seq(
      "b" -> (5, 6),
      "b" -> (5, 7),
    )

    val forA = results.filter(_._1 === "a")
    val forB = results.filter(_._1 === "b")

    forA should contain theSameElementsAs expectedA
    forB should contain theSameElementsAs expectedB

    // Only keys present in both sides should appear
    results.map(_._1).toSet shouldBe Set("a", "b")
  }

  it should "return empty results when either side is empty" in {
    // Arrange
    ScarletRuntime.get.shuffle.clear()

    val nonEmpty = toDistCollection(Seq("x" -> 1, "y" -> 2))
    val emptyDC  = toDistCollection(Seq.empty[(String, Int)])

    // Act + Assert
    nonEmpty.join(emptyDC).collect().toSeq shouldBe empty
    emptyDC.join(nonEmpty).collect().toSeq shouldBe empty
  }

  it should "match a scalar left outer join reference" in {
    val cases: Seq[(String, Seq[(String, Int)], Seq[(String, Int)])] = Seq(
      (
        "matches and unmatched left rows",
        Seq("a" -> 1, "b" -> 7, "a" -> 3),
        Seq("a" -> 2, "a" -> 4, "c" -> 9),
      ),
      (
        "duplicate keys",
        Seq("a" -> 1, "a" -> 3, "b" -> 5),
        Seq("a" -> 2, "a" -> 4, "b" -> 6, "b" -> 7),
      ),
      (
        "null key on the left",
        Seq(nullKey(1), "a" -> 2, nullKey(4)),
        Seq("a" -> 3, "b" -> 8),
      ),
      (
        "null key on the right",
        Seq("a" -> 1, "b" -> 2),
        Seq(nullKey(9), "a" -> 3),
      ),
      (
        "null keys on both sides",
        Seq(nullKey(1), "a" -> 2, nullKey(4)),
        Seq(nullKey(9), nullKey(8), "a" -> 3),
      ),
      (
        "empty right side",
        Seq("x" -> 1, "y" -> 2, "x" -> 1),
        Seq.empty[(String, Int)],
      ),
      (
        "empty left side",
        Seq.empty[(String, Int)],
        Seq("x" -> 1, "y" -> 2),
      ),
    )

    cases.foreach { case (name, leftRows, rightRows) =>
      ScarletRuntime.get.shuffle.clear()
      val actual = toDistCollection(leftRows).leftOuterJoin(toDistCollection(rightRows)).collect().toSeq
      withClue(name) {
        actual should contain theSameElementsAs referenceLeftOuter(leftRows, rightRows)
      }
    }
  }

  it should "plan a wide left outer join that the columnar planner refuses" in {
    ScarletRuntime.get.shuffle.clear()
    val left = toDistCollection(Seq("a" -> 1))
    val right = toDistCollection(Seq("a" -> 2, "b" -> 3))
    val plan = left.leftOuterJoin(right).plan

    plan shouldBe a[Plan.LeftOuterJoinOp[_, _, _]]
    plan match {
      case Plan.LeftOuterJoinOp(_, _, strategy) => strategy shouldBe None
      case _ => fail("Expected LeftOuterJoinOp")
    }
    PlanWide.isWide(plan) shouldBe true
    PlanWide.isDirectlyWide(plan) shouldBe true
    leftOuterKinds(plan) shouldBe Set(WideOpKind.LeftOuterJoin)

    val saved = ScarletConf.get
    try {
      ScarletConf.set(saved.copy(columnarExecution = true))
      ColumnarPlanner.lower(plan) shouldBe None
    } finally ScarletConf.set(saved)
  }

  it should "drop a null key before grouping" in {
    val rows = Seq(nullKey(1), "a" -> 2, nullKey(4), "a" -> 5)
    val onlyNull = Seq(nullKey(1), nullKey(2))

    ScarletRuntime.get.shuffle.clear()
    val grouped = toDistCollection(rows).groupByKey.collect().toSeq
    grouped.map(_._1) should contain theSameElementsAs Seq("a")
    grouped.flatMap(_._2) should contain theSameElementsAs Seq(2, 5)
    toDistCollection(onlyNull).groupByKey.collect() shouldBe empty

    ScarletRuntime.get.shuffle.clear()
    val reduced = toDistCollection(rows).reduceByKey[String, Int](_ + _).collect().toSeq
    reduced should contain theSameElementsAs Seq("a" -> 7)
    toDistCollection(onlyNull).reduceByKey[String, Int](_ + _).collect() shouldBe empty

    ScarletRuntime.get.shuffle.clear()
    val bypassed = toDistCollection(rows)
      .partitionBy[String, Int](ScarletConf.get.defaultShufflePartitions)
      .groupByKey
      .collect()
      .toSeq
    bypassed.map(_._1) should contain theSameElementsAs Seq("a")
    bypassed.flatMap(_._2) should contain theSameElementsAs Seq(2, 5)

    ScarletRuntime.get.shuffle.clear()
    val cogrouped = toDistCollection(Seq(nullKey(1), "a" -> 2))
      .cogroup(toDistCollection(Seq(nullKey(9), "b" -> 3, "a" -> 4)))
      .collect()
      .toSeq
    cogrouped.map(_._1) should contain theSameElementsAs Seq("a", "b")
    val byKey = cogrouped.map { case (key, (left, right)) =>
      key -> (left.toSeq, right.toSeq)
    }.toMap
    byKey("a") shouldBe (Seq(2), Seq(4))
    byKey("b") shouldBe (Seq.empty[Int], Seq(3))
  }

  it should "drop null keys from every inner join strategy" in {
    val cases = Seq(
      (
        "null key on the left",
        Seq(nullKey(1), "a" -> 2),
        Seq("a" -> 3, "b" -> 8),
        Seq("a" -> (2, 3)),
      ),
      (
        "null key on the right",
        Seq("a" -> 2, "b" -> 1),
        Seq(nullKey(9), "a" -> 3),
        Seq("a" -> (2, 3)),
      ),
      (
        "null keys on both sides",
        Seq(nullKey(1), "a" -> 2, nullKey(4)),
        Seq(nullKey(9), nullKey(8), "a" -> 3),
        Seq("a" -> (2, 3)),
      ),
      (
        "only null keys",
        Seq(nullKey(1), nullKey(4)),
        Seq(nullKey(9)),
        Seq.empty[(String, (Int, Int))],
      ),
    )
    val strategies: Seq[
      (
          String,
          (DistCollection[(String, Int)], DistCollection[(String, Int)]) => DistCollection[
            (String, (Int, Int)),
          ],
      ),
    ] = Seq(
      ("join", (left, right) => left.join(right)),
      ("shuffle-hash", (left, right) => left.shuffleHashJoin(right)),
      ("broadcast", (left, right) => left.broadcastJoin(right)),
      ("sort-merge", (left, right) => left.sortMergeJoin(right)),
    )

    strategies.foreach { case (strategy, join) =>
      cases.foreach { case (name, leftRows, rightRows, expected) =>
        ScarletRuntime.get.shuffle.clear()
        val actual = join(toDistCollection(leftRows), toDistCollection(rightRows)).collect().toSeq
        withClue(s"$strategy / $name") {
          actual should contain theSameElementsAs expected
        }
      }
    }
  }

  it should "keep a left null key with None when a strategy hint is set" in {
    val leftRows = Seq(nullKey(1), "a" -> 2, nullKey(4))
    val rightRows = Seq(nullKey(9), nullKey(8), "a" -> 3)
    val expected = Seq(leftNull(1), "a" -> (2, Some(3)), leftNull(4))
    val hints = Seq(
      None,
      Some(Plan.JoinStrategy.Broadcast),
      Some(Plan.JoinStrategy.SortMerge),
      Some(Plan.JoinStrategy.ShuffleHash),
    )

    hints.foreach { hint =>
      ScarletRuntime.get.shuffle.clear()
      val plan = Plan.LeftOuterJoinOp(
        Plan.Source(Seq(Partition(leftRows))),
        Plan.Source(Seq(Partition(rightRows))),
        hint,
      )
      withClue(hint) {
        leftOuterKinds(plan) shouldBe Set(WideOpKind.LeftOuterJoin)
        DistCollection(plan).collect().toSeq should contain theSameElementsAs expected
      }
    }
  }

  private def leftOuterKinds(plan: Plan[_]): Set[WideOpKind] =
    StageBuilder
      .buildStageGraph(plan)
      .stages
      .values
      .flatMap(_.wideOp)
      .collect { case op: LeftOuterJoinWideOp => op.meta.kind }
      .toSet

  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Equals"))
  private def nullKey(value: Int): (String, Int) = (null: String) -> value

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def leftNull(value: Int): (String, (Int, Option[Int])) =
    ((null: String), (value, None))

  /** Scalar left outer join. A null key does not match, and each unmatched left row is kept once. */
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Equals"))
  private def referenceLeftOuter(
      left: Seq[(String, Int)],
      right: Seq[(String, Int)],
  ): Seq[(String, (Int, Option[Int]))] =
    left.flatMap { case (key, leftValue) =>
      val matches =
        if (key == null) Seq.empty[Int]
        else right.collect { case (rightKey, rightValue) if rightKey == key => rightValue }
      if (matches.isEmpty) Seq((key, (leftValue, None)))
      else matches.map(rightValue => (key, (leftValue, Some(rightValue))))
    }
}


