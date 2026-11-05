package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestGroupByKey extends AnyFlatSpec with Matchers:

  private val firstKeys = Array(1, 2, -1, 4, Int.MinValue, 1, 2)
  private val firstValues = Array(10, 20, 30, 40, 50, 11, 21)
  private val secondKeys = Array(1, 4)
  private val secondValues = Array(12, 41)

  "groupByKey" should "keep first-seen keys and input value order across batches" in {
    val parts = group(partitions = 4, parallelism = 2)

    parts.length shouldBe 4
    rowsOf(parts(0)) shouldBe Seq((4, Seq(40, 41)), (Int.MinValue, Seq(50)))
    rowsOf(parts(1)) shouldBe Seq((1, Seq(10, 11, 12)))
    rowsOf(parts(2)) shouldBe Seq((2, Seq(20, 21)))
    rowsOf(parts(3)) shouldBe Seq((-1, Seq(30)))
    Math.floorMod(Int.MinValue, 4) shouldBe 0
    parts.foreach(assertBatch)
    val repeated = parts(1).valuesFor(0)
    repeated shouldBe List(10, 11, 12)
    repeated.toList shouldBe List(10, 11, 12)
  }

  it should "group one partition in first-seen order" in {
    val parts = group(partitions = 1, parallelism = 1)

    parts.length shouldBe 1
    rowsOf(parts.headOption.getOrElse(fail("missing partition"))) shouldBe Seq(
      (1, Seq(10, 11, 12)),
      (2, Seq(20, 21)),
      (-1, Seq(30)),
      (4, Seq(40, 41)),
      (Int.MinValue, Seq(50)),
    )
  }

  it should "drop a null key and keep the other values in order" in {
    val keys = Int32Column.of(Array(1, 2, 1), Validity.pack(3, row => row != 1), 3)
    val values = Int32Column.of(Array(10, 99, 11), Validity.allValid(3), 3)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(keys, values),
      3,
    )
    val parts = GroupByKey.int32(Vector(batch), partitions = 4, parallelism = 1)

    parts.length shouldBe 4
    rowsOf(parts(1)) shouldBe Seq((1, Seq(10, 11)))
    parts.zipWithIndex.foreach { (grouped, index) =>
      if (index == 1) then grouped.groups shouldBe 1
      else grouped.groups shouldBe 0
    }
    parts.foreach(assertBatch)
  }

  it should "reject a clear value bit, including one beside a null key" in {
    val presentKey = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(Array(1, 1), Validity.allValid(2), 2),
        Int32Column.of(Array(10, 20), Validity.pack(2, row => row != 1), 2),
      ),
      2,
    )
    val nullKey = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(Array(2, 1), Validity.pack(2, row => row != 0), 2),
        Int32Column.of(Array(99, 10), Validity.pack(2, row => row != 0), 2),
      ),
      2,
    )

    val present = the[IllegalArgumentException] thrownBy {
      GroupByKey.int32(Vector(presentKey), partitions = 2, parallelism = 1)
    }
    val absentKey = the[IllegalArgumentException] thrownBy {
      GroupByKey.int32(Vector(nullKey), partitions = 2, parallelism = 1)
    }
    present.getMessage should include("clear value bit")
    absentKey.getMessage should include("clear value bit")
  }

  it should "publish an empty batch for every partition of an empty input" in {
    val missing = GroupByKey.int32(Vector.empty, partitions = 4, parallelism = 2)
    val emptyBatch = BatchCodec.intPairs(Seq.empty[(Int, Int)])
    val present = GroupByKey.int32(Vector(emptyBatch), partitions = 3, parallelism = 1)

    missing.length shouldBe 4
    present.length shouldBe 3
    (missing ++ present).foreach { batch =>
      batch.groups shouldBe 0
      batch.length shouldBe 0
      batch.offsets.toSeq shouldBe Seq(0)
      batch.values.toSeq shouldBe Seq.empty[Int]
      batch.rows shouldBe empty
      assertBatch(batch)
    }
  }

  it should "publish empty partitions beside a populated one" in {
    val batch = BatchCodec.intPairs(Seq((4, 40), (8, 80), (4, 41)))
    val parts = GroupByKey.int32(Vector(batch), partitions = 4, parallelism = 2)

    parts.length shouldBe 4
    rowsOf(parts(0)) shouldBe Seq((4, Seq(40, 41)), (8, Seq(80)))
    parts.drop(1).foreach { grouped =>
      grouped.groups shouldBe 0
      grouped.offsets.toSeq shouldBe Seq(0)
    }
  }

  it should "expose each group as a view of the values array" in {
    val batch = group(partitions = 4, parallelism = 1)(0)
    val view = batch.valuesFor(0)
    val start = batch.offsets(0)
    val before = batch.values(start)

    view shouldBe List(40, 41)
    view.toList shouldBe view.toList
    batch.values(start) = before + 7
    view(0) shouldBe before + 7
    batch.values(start) = before
    view(0) shouldBe before
    an[IllegalArgumentException] should be thrownBy batch.valuesFor(-1)
    an[IllegalArgumentException] should be thrownBy batch.valuesFor(batch.groups)
    an[IndexOutOfBoundsException] should be thrownBy view(-1)
    an[IndexOutOfBoundsException] should be thrownBy view(view.length)
  }

  private def group(partitions: Int, parallelism: Int): Vector[GroupedIntBatch] =
    GroupByKey.int32(
      Vector(
        BatchCodec.intPairs(firstKeys.zip(firstValues)),
        BatchCodec.intPairs(secondKeys.zip(secondValues)),
      ),
      partitions,
      parallelism,
    )

  private def rowsOf(batch: GroupedIntBatch): Seq[(Int, Seq[Int])] =
    batch.rows.map { case (key, values) => (key, values.toSeq) }

  private def assertBatch(batch: GroupedIntBatch): Unit =
    val offsets = batch.offsets.toSeq
    offsets shouldBe offsets.sorted
    offsets.headOption shouldBe Some(0)
    batch.offsets(batch.groups) shouldBe batch.values.length
    batch.keys.length shouldBe batch.groups
    (0 until batch.groups).foreach { group =>
      val values = batch.valuesFor(group)
      val slice = batch.values.slice(batch.offsets(group), batch.offsets(group + 1))
      values shouldBe slice.toIndexedSeq
    }
