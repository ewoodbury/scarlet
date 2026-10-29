package com.ewoodbury.scarlet.columnar

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.execution.ColumnarPlanner
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/** Inner sort-merge of int pairs against the row path, including the merge order. */
class TestSortMergeJoin extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "sort-merge" should "match the row path on hits, duplicates, and extreme keys" in {
    agree(
      Seq(1 -> 10, 2 -> 20, 4 -> 40),
      Seq(1 -> 100, 3 -> 30, 2 -> 200),
    )
    agree(
      Seq(1 -> 10, 1 -> 11, 2 -> 20),
      Seq(1 -> 100, 1 -> 101, 1 -> 102, 3 -> 30),
    )
    agree(
      Seq(Int.MinValue -> 1, -8 -> 2, -1 -> 3, 0 -> 4, Int.MaxValue -> 5),
      Seq(Int.MinValue -> 10, -1 -> 30, 4 -> 40, Int.MaxValue -> 50),
    )
    agree(
      Seq(-5 -> 1, -1 -> 2, 0 -> 3, 3 -> 4),
      Seq(-5 -> 10, -1 -> 11, -1 -> 12, 3 -> 30, 8 -> 80),
    )
  }

  it should "return no rows when either side is empty" in {
    val emptyPairs = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 2)
    val present = DistCollection.ofIntPairs(Array(1, -4), Array(2, 5), 2)
    val noRows = Seq.empty[(Int, (Int, Int))]

    ColumnarPlanner.lower(emptyPairs.sortMergeJoin(present).plan) shouldBe defined
    collect(emptyPairs.sortMergeJoin(present)) shouldBe noRows
    collect(present.sortMergeJoin(emptyPairs)) shouldBe noRows
    collect(emptyPairs.sortMergeJoin(emptyPairs)) shouldBe noRows
    rowCollect(emptyPairs.sortMergeJoin(present).plan) shouldBe noRows
  }

  it should "drop a null key and keep a null value" in {
    val left = Seq(
      (None, Some(99)),
      (Some(0), Some(1)),
      (Some(2), None),
      (Some(4), Some(8)),
    )
    val right = Seq(
      (None, Some(7)),
      (Some(0), Some(5)),
      (Some(2), Some(6)),
      (Some(3), Some(3)),
    )
    val expected = scalar(left, right)
    val joined = SortMerge.join(nullable(left), nullable(right))

    optional(joined) shouldBe expected
    optional(joined).map(_._2) should not contain Some(99)
    optional(joined).map(_._3) should not contain Some(7)
    expected should contain theSameElementsAs Seq((0, Some(1), Some(5)), (2, None, Some(6)))

    val parts = ColumnPipeline.sortMergeJoin(
      Vector(nullable(left)),
      Vector(nullable(right)),
      numPartitions = 4,
      parallelism = 2,
      batchSize = 1,
    )
    parts.flatMap(optional).sorted shouldBe expected.sorted

    val leftKeys = Seq(erased(None, Some(99)), erased(Some(0), Some(1)), erased(Some(4), Some(8)))
    val rightKeys = Seq(erased(None, Some(7)), erased(Some(0), Some(5)), erased(Some(3), Some(3)))
    val keyOnly = SortMerge.join(
      nullable(Seq((None, Some(99)), (Some(0), Some(1)), (Some(4), Some(8)))),
      nullable(Seq((None, Some(7)), (Some(0), Some(5)), (Some(3), Some(3)))),
    )
    BatchCodec.decodeIntTriples(keyOnly) should contain theSameElementsAs rowPairs(leftKeys, rightKeys)
    rowPairs(leftKeys, rightKeys) should contain theSameElementsAs Seq(0 -> ((1, 5)))
  }

  it should "keep an empty partition index while another partition matches" in {
    val left = BatchCodec.intPairs(Seq((0, 1), (2, 2)))
    val right = BatchCodec.intPairs(Seq((0, 10), (1, 11)))
    val parts = ColumnPipeline.sortMergeJoin(
      Vector(left),
      Vector(right),
      numPartitions = 2,
      parallelism = 1,
      batchSize = 8,
    )

    parts.length shouldBe 2
    val idle = parts.lift(Math.floorMod(1, 2)).getOrElse(fail("missing partition"))
    val hit = parts.lift(Math.floorMod(0, 2)).getOrElse(fail("missing partition"))
    idle.length shouldBe 0
    BatchCodec.decodeIntTriples(hit) shouldBe Seq(0 -> ((1, 10)))
    nonDecreasing(parts)

    val zipped = SortMerge.joinAll(
      Vector(BatchCodec.intPairs(Seq.empty), BatchCodec.intPairs(Seq(4 -> 7))),
      Vector(BatchCodec.intPairs(Seq.empty), BatchCodec.intPairs(Seq(4 -> 8, 4 -> 9))),
    )
    zipped.length shouldBe 2
    zipped.lift(0).getOrElse(fail("missing partition")).length shouldBe 0
    BatchCodec.decodeIntTriples(zipped.lift(1).getOrElse(fail("missing partition"))) shouldBe Seq(
      4 -> ((7, 8)),
      4 -> ((7, 9)),
    )

    agree(Seq(0 -> 1, 2 -> 2), Seq(0 -> 10, 1 -> 11), parts = 2)
  }

  it should "merge a duplicate key that spans several morsels" in {
    val batchSize = 4
    val copies = 24
    copies should be > batchSize * 2
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = 4,
        threadPoolSize = 2,
      ),
    )
    val left = (0 until copies).map(row => 11 -> row)
    val right = (0 until 6).map(row => 11 -> (100 + row))
    val plan = DistCollection(left, 1).sortMergeJoin(DistCollection(right, 1)).plan
    val parts = ColumnarPlanner.tryRun(plan).getOrElse(fail("sort-merge was refused"))

    parts.length shouldBe 4
    val home = Math.floorMod(11, 4)
    val matched = parts.lift(home).getOrElse(fail("missing partition"))
    matched.data.size shouldBe copies * 6
    parts.zipWithIndex.foreach { (part, index) =>
      if (index != home) then part.data shouldBe empty
    }
    nonDecreasing(parts)
    parts.flatMap(_.data) should contain theSameElementsAs rowPairs(left, right)
  }

  it should "emit sorted keys from descending input" in {
    val left = Seq(
      (5, 50),
      (5, 51),
      (3, 30),
      (1, 10),
      (1, 11),
      (-2, 8),
      (Int.MinValue, 7),
    )
    val right = Seq(
      (Int.MaxValue, 99),
      (4, 40),
      (1, 100),
      (1, 101),
      (-2, 9),
      (Int.MinValue, 70),
    )
    val inputKeys = left.map(_._1)
    val descending = inputKeys.zip(inputKeys.drop(1)).exists { (previous, next) =>
      Integer.compare(previous, next) > 0
    }
    descending shouldBe true

    val joined = SortMerge.join(pairs(left), pairs(right))
    val actual = BatchCodec.decodeIntTriples(joined)
    val expected = scalar(
      left.map { case (key, value) => (Some(key), Some(value)) },
      right.map { case (key, value) => (Some(key), Some(value)) },
    )

    actual shouldBe presentTriples(expected)
    nonDecreasing(Vector(joined))

    ScarletConf.set(originalConf.copy(columnarExecution = true, defaultShufflePartitions = 4))
    val plan = DistCollection(left, 2).sortMergeJoin(DistCollection(right, 2)).plan
    val parts = ColumnarPlanner.tryRun(plan).getOrElse(fail("sort-merge was refused"))
    parts.length shouldBe 4
    nonDecreasing(parts)
    val flat = parts.flatMap(_.data)
    flat should contain theSameElementsAs actual
    val keys = flat.map(_._1)
    val smallest = keys.foldLeft(Int.MaxValue)(math.min)
    val largest = keys.foldLeft(Int.MinValue)(math.max)
    largest should be > smallest
  }

  it should "accept sort-merge only when both sides are int pairs" in {
    val strings = DistCollection(Seq("a" -> 1), 1).sortMergeJoin(DistCollection(Seq("a" -> 2), 1))
    val doubles = DistCollection(Seq(1.0 -> 1), 1).sortMergeJoin(DistCollection(Seq(1.0 -> 2), 1))
    val pairs = DistCollection.ofIntPairs(Array(1), Array(2), 1).plan
    val outers = Seq(
      Plan.LeftOuterJoinOp(pairs, pairs, Some(Plan.JoinStrategy.SortMerge)),
      Plan.RightOuterJoinOp(pairs, pairs, Some(Plan.JoinStrategy.SortMerge)),
      Plan.FullOuterJoinOp(pairs, pairs, Some(Plan.JoinStrategy.SortMerge)),
    )
    val shapes = Seq(
      "SortMergeLeftOuter(Exchange(Scan), Exchange(Scan))",
      "SortMergeRightOuter(Exchange(Scan), Exchange(Scan))",
      "SortMergeFullOuter(Exchange(Scan), Exchange(Scan))",
    )

    ColumnarPlanner.lower(strings.plan) shouldBe None
    ColumnarPlanner.lower(doubles.plan) shouldBe None
    refuseOuter(
      DistCollection(Seq("a" -> 1), 1).plan,
      DistCollection(Seq("a" -> 2), 1).plan,
    )
    refuseOuter(
      DistCollection(Seq(1.0 -> 1), 1).plan,
      DistCollection(Seq(1.0 -> 2), 1).plan,
    )
    outers.zip(shapes).foreach { (plan, shape) =>
      ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(shape)
    }
    ColumnarPlanner.tryTake(strings.plan, 1) shouldBe None
    val hinted = DistCollection.ofIntPairs(Array(1), Array(2), 1)
      .sortMergeJoin(DistCollection.ofIntPairs(Array(1), Array(3), 1))
    ColumnarPlanner.tryTake(hinted.plan, 1) shouldBe None
    ColumnarPlanner.lower(hinted.plan).map(_.shape) shouldBe Some(
      "SortMergeJoin(Exchange(Scan), Exchange(Scan))",
    )
    val plain = DistCollection.ofIntPairs(Array(1), Array(2), 1)
      .join(DistCollection.ofIntPairs(Array(1), Array(3), 1))
    ColumnarPlanner.lower(plain.plan).map(_.shape) shouldBe Some(
      "HashJoin(Exchange(Scan), Exchange(Scan))",
    )
  }

  private def refuseOuter[K, V, W](left: Plan[(K, V)], right: Plan[(K, W)]): Unit =
    val hinted = Some(Plan.JoinStrategy.SortMerge)
    Seq(
      Plan.LeftOuterJoinOp(left, right, hinted),
      Plan.RightOuterJoinOp(left, right, hinted),
      Plan.FullOuterJoinOp(left, right, hinted),
    ).foreach { outer =>
      ColumnarPlanner.lower(outer) shouldBe None
    }

  private def agree(left: Seq[(Int, Int)], right: Seq[(Int, Int)], parts: Int): Unit =
    columnarPairs(left, right, parts) should contain theSameElementsAs rowPairs(left, right, parts)

  private def agree(left: Seq[(Int, Int)], right: Seq[(Int, Int)]): Unit =
    agree(left, right, parts = 2)

  private def columnarPairs(
      left: Seq[(Int, Int)],
      right: Seq[(Int, Int)],
      parts: Int,
  ): Seq[(Int, (Int, Int))] =
    ScarletRuntime.get.shuffle.clear()
    DistCollection(left, parts).sortMergeJoin(DistCollection(right, parts)).collect().toSeq

  private def rowPairs(
      left: Seq[(Int, Int)],
      right: Seq[(Int, Int)],
      parts: Int = 2,
  ): Seq[(Int, (Int, Int))] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      DistCollection(left, parts).sortMergeJoin(DistCollection(right, parts)).collect().toSeq
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def rowCollect(plan: Plan[(Int, (Int, Int))]): Seq[(Int, (Int, Int))] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      DistCollection(plan).collect().toSeq
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def collect(collection: DistCollection[(Int, (Int, Int))]): Seq[(Int, (Int, Int))] =
    ScarletRuntime.get.shuffle.clear()
    collection.collect().toSeq

  private def pairs(rows: Seq[(Int, Int)]): ColumnBatch =
    BatchCodec.intPairs(rows)

  private def nullable(rows: Seq[(Option[Int], Option[Int])]): ColumnBatch =
    BatchCodec.nullableIntPairs(rows)

  /**
   * Named erasure boundary. A null key is a null reference inside an `(Int, Int)` tuple, which is
   * how the row join observes one. The cast restores that tuple type.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def erased(key: Option[Int], value: Option[Int]): (Int, Int) =
    val left: AnyRef = key match
      case Some(present) => Int.box(present)
      case None => null
    val right: AnyRef = value match
      case Some(present) => Int.box(present)
      case None => null
    Tuple2(left, right).asInstanceOf[(Int, Int)]

  private def presentTriples(
      rows: Seq[(Int, Option[Int], Option[Int])],
  ): Seq[(Int, (Int, Int))] =
    rows.flatMap { (key, left, right) =>
      (left, right) match
        case (Some(leftValue), Some(rightValue)) => Some(key -> ((leftValue, rightValue)))
        case (Some(_), None) | (None, Some(_)) | (None, None) => None
    }

  private def optional(batch: ColumnBatch): Seq[(Int, Option[Int], Option[Int])] =
    val keys = int32(batch, 0)
    val left = int32(batch, 1)
    val right = int32(batch, 2)
    (0 until batch.length).map { row =>
      (keys.values(row), cell(left, row), cell(right, row))
    }

  private def cell(column: Int32Column, row: Int): Option[Int] =
    if (column.isValid(row)) then Some(column.values(row)) else None

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  private def nonDecreasing(parts: Seq[Partition[(Int, (Int, Int))]]): Unit =
    parts.foreach { part =>
      val keys = part.data.map(_._1)
      keys.zip(keys.drop(1)).foreach { (previous, next) =>
        Integer.compare(previous, next) should be <= 0
      }
    }

  private def nonDecreasing(parts: Vector[ColumnBatch]): Unit =
    parts.foreach { batch =>
      val keys = int32(batch, 0)
      (1 until batch.length).foreach { row =>
        Integer.compare(keys.values(row - 1), keys.values(row)) should be <= 0
      }
    }

  /** Independent merge. Null keys are dropped. Equal keys keep sort order and cross. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def scalar(
      left: Seq[(Option[Int], Option[Int])],
      right: Seq[(Option[Int], Option[Int])],
  ): Seq[(Int, Option[Int], Option[Int])] =
    val leftRows = left.collect { case (Some(key), value) => (key, value) }.sortBy(_._1).toArray
    val rightRows = right.collect { case (Some(key), value) => (key, value) }.sortBy(_._1).toArray
    var acc = List.empty[(Int, Option[Int], Option[Int])]
    var leftAt = 0
    var rightAt = 0
    while (leftAt < leftRows.length && rightAt < rightRows.length) {
      val order = Integer.compare(leftRows(leftAt)._1, rightRows(rightAt)._1)
      if (order < 0) then leftAt = scalarGroupEnd(leftRows, leftAt)
      else if (order > 0) then rightAt = scalarGroupEnd(rightRows, rightAt)
      else
        val leftEnd = scalarGroupEnd(leftRows, leftAt)
        val rightEnd = scalarGroupEnd(rightRows, rightAt)
        var leftRow = leftAt
        while (leftRow < leftEnd) {
          var rightRow = rightAt
          while (rightRow < rightEnd) {
            val leftPair = leftRows(leftRow)
            val rightPair = rightRows(rightRow)
            acc = (leftPair._1, leftPair._2, rightPair._2) :: acc
            rightRow += 1
          }
          leftRow += 1
        }
        leftAt = leftEnd
        rightAt = rightEnd
    }
    acc.reverse

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def scalarGroupEnd(rows: Array[(Int, Option[Int])], start: Int): Int =
    val key = rows(start)._1
    var end = start + 1
    while (end < rows.length && rows(end)._1 == key) {
      end += 1
    }
    end
