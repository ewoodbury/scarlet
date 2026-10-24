package com.ewoodbury.scarlet.columnar

import scala.util.Random

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestMergeSumPartials extends AnyFlatSpec with Matchers:

  "merge" should "match concat plus sumInt64 on a mixed fixture" in {
    val random = new Random(11)
    val dense = partial(Seq((Some(1), Some(4)), (Some(2), Some(5)), (Some(1), Some(6))))
    val sparse = partial(
      Seq((Some(2), None), (Some(3), Some(1)), (Some(4), None), (Some(3), Some(2))),
    )
    val generated = Vector.fill(8)(generatedPartial(random))
    val partials = dense +: sparse +: generated :+ partial(Seq.empty)
    val folded = HashAggregate.mergeSumPartials(partials, keyOrdinal = 0, valueOrdinal = 1)
    val legacy = HashAggregate.sumInt64(ColumnBatches.concat(partials), keyOrdinal = 0, valueOrdinal = 1)

    fullyPresent(dense) shouldBe true
    fullyPresent(sparse) shouldBe false
    keysOf(folded) shouldBe keysOf(legacy)
    sums(folded) shouldBe sums(legacy)
  }

  it should "keep input batches apart when their keys are disjoint" in {
    val left = BatchCodec.intPairs(Seq((1, 3), (1, 4), (3, 1)))
    val right = BatchCodec.intPairs(Seq((2, 5), (2, 6), (4, 8)))
    Seq(1, 4).foreach { parallelism =>
      run(Vector(left, right), parallelism = parallelism, batchSize = 1) match
        case Vector(leftPartial, rightPartial) =>
          sums(leftPartial) shouldBe Map(1 -> Some(7L), 3 -> Some(1L))
          sums(rightPartial) shouldBe Map(2 -> Some(11L), 4 -> Some(8L))
          keysOf(leftPartial) shouldBe Seq(1, 3)
          keysOf(rightPartial) shouldBe Seq(2, 4)
        case other => fail(s"expected two partials, got ${other.length}")
    }
  }

  it should "group partials without concatenating them" in {
    val first = partial(Seq((Some(1), Some(1))))
    val second = partial(Seq((Some(2), Some(2))))
    val third = partial(Seq((Some(3), Some(3))))
    val groups = PartialCombine.groupPartials(Vector(2, 1), Vector(first, second, third))

    groups match
      case Vector(Vector(left, middle), Vector(right)) =>
        left should be theSameInstanceAs first
        middle should be theSameInstanceAs second
        right should be theSameInstanceAs third
      case other => fail(s"expected groups of 2 and 1, got ${other.map(_.length)}")
  }

  it should "round-trip a null sum across partials, including a dense neighbour" in {
    val nullThenValue = partial(Seq((Some(1), None), (Some(2), Some(5)), (Some(3), None)))
    val valueThenNull = partial(Seq((Some(1), Some(4)), (Some(2), None)))
    val folded = HashAggregate.mergeSumPartials(
      Vector(nullThenValue, valueThenNull),
      keyOrdinal = 0,
      valueOrdinal = 1,
    )

    fullyPresent(nullThenValue) shouldBe false
    keysOf(folded) shouldBe Seq(1, 2, 3)
    sums(folded) shouldBe Map(1 -> Some(4L), 2 -> Some(5L), 3 -> None)

    val dense = partial(Seq((Some(10), Some(7))))
    val nullOnly = partial(Seq((Some(10), None), (Some(11), None)))
    val mixed = HashAggregate.mergeSumPartials(Vector(dense, nullOnly), keyOrdinal = 0, valueOrdinal = 1)

    fullyPresent(dense) shouldBe true
    fullyPresent(nullOnly) shouldBe false
    keysOf(mixed) shouldBe Seq(10, 11)
    sums(mixed) shouldBe Map(10 -> Some(7L), 11 -> None)
  }

  it should "not grow when 1024 groups fill the table and a duplicate partial follows" in {
    val width = 512
    val first = block(0, width)
    val second = block(width, width)
    val duplicate = block(0, width)
    val merged = HashAggregate.mergeSumPartials(
      Vector(first, second, duplicate),
      keyOrdinal = 0,
      valueOrdinal = 1,
    )

    HashAggregate.mergeSumTableCapacity shouldBe ColumnHash.tableSize(2 * width)
    merged.length shouldBe 1024
    keysOf(merged) shouldBe (0 until 1024)
    sums(merged) shouldBe ((0 until 512).map(key => key -> Some(2L)) ++
      (512 until 1024).map(key => key -> Some(1L))).toMap
  }

  it should "seed 1024-row partials from the widest partial and not from the concatenated rows" in {
    val rows = 1024
    val copies = 4
    val partials = Vector.fill(copies)(block(0, rows))
    val merged = HashAggregate.mergeSumPartials(partials, keyOrdinal = 0, valueOrdinal = 1)
    val seeded = ColumnHash.tableSize(2 * rows)

    HashAggregate.mergeSumTableCapacity shouldBe seeded
    ColumnHash.tableSize(rows * copies) should be > seeded
    merged.length shouldBe rows
    sums(merged).values.toSet shouldBe Set(Some(copies.toLong))
  }

  it should "grow when later partials add groups the first partial did not have" in {
    val narrow = block(0, 4)
    val wide = 16
    val later = Vector(block(100, wide), block(200, wide), block(300, wide))
    // The repeated prefix follows the grow. Its slots must be found again.
    val merged =
      HashAggregate.mergeSumPartials(narrow +: later :+ narrow, keyOrdinal = 0, valueOrdinal = 1)
    val seeded = ColumnHash.tableSize(2 * wide)

    narrow.length should be < wide
    HashAggregate.mergeSumTableCapacity shouldBe (seeded << 1)
    merged.length shouldBe 4 + wide * 3
    sums(merged) shouldBe (
      (0 until 4).map(key => key -> Some(2L)) ++
        (100 until 116).map(key => key -> Some(1L)) ++
        (200 until 216).map(key => key -> Some(1L)) ++
        (300 until 316).map(key => key -> Some(1L))
    ).toMap
    keysOf(merged) shouldBe (0 until 4) ++ (100 until 116) ++ (200 until 216) ++ (300 until 316)
  }

  it should "not grow the worker table when a duplicate arrives at its load limit" in {
    val rows = (0 until 8).map(key => (key, 1)) ++ Seq((0, 2), (1, 3))
    val batch = BatchCodec.intPairs(rows)
    HashAggregate.releaseSumWorkers()
    try
      val summed = HashAggregate.sumRange(int32(batch, 0), int32(batch, 1), start = 0, length = batch.length)
      HashAggregate.sumWorkerCapacity shouldBe HashAggregate.SumTableStart
      sums(summed) shouldBe Map(
        0 -> Some(3L),
        1 -> Some(4L),
        2 -> Some(1L),
        3 -> Some(1L),
        4 -> Some(1L),
        5 -> Some(1L),
        6 -> Some(1L),
        7 -> Some(1L),
      )
    finally HashAggregate.releaseSumWorkers()
  }

  "lost groups" should "keep both keys of a two-row batch in first-seen order" in {
    val batch = BatchCodec.intPairs(Seq((1, 10), (2, 20)))
    val merged = one(run(Vector(batch), parallelism = 1, batchSize = 1))

    keysOf(merged) shouldBe Seq(1, 2)
    sums(merged) shouldBe Map(1 -> Some(10L), 2 -> Some(20L))
  }

  it should "keep a null key beside a valued key when each row is its own morsel" in {
    val batch = BatchCodec.nullableIntPairs(Seq((Some(1), None), (Some(2), Some(5))))
    val merged = one(run(Vector(batch), parallelism = 1, batchSize = 1))

    keysOf(merged) shouldBe Seq(1, 2)
    sums(merged) shouldBe Map(1 -> None, 2 -> Some(5L))
  }

  "edges" should "fold one partial, no partials, and a single group" in {
    val only = partial(Seq((Some(4), Some(3)), (Some(4), Some(1)), (Some(8), None)))
    val folded = HashAggregate.mergeSumPartials(Vector(only), keyOrdinal = 0, valueOrdinal = 1)
    val direct = HashAggregate.sumInt64(only, keyOrdinal = 0, valueOrdinal = 1)

    keysOf(folded) shouldBe keysOf(direct)
    sums(folded) shouldBe sums(direct)

    val empty = HashAggregate.mergeSumPartials(Vector.empty, keyOrdinal = 0, valueOrdinal = 1)
    empty.length shouldBe 0
    empty.schema shouldBe Vector(LogicalType.Int32, LogicalType.Int64)
    sums(empty) shouldBe Map.empty[Int, Option[Long]]

    val group = HashAggregate.mergeSumPartials(
      Vector(
        partial(Seq((Some(1), Some(2)), (Some(1), Some(3)))),
        partial(Seq((Some(1), None))),
        partial(Seq((Some(1), Some(4)))),
      ),
      keyOrdinal = 0,
      valueOrdinal = 1,
    )
    keysOf(group) shouldBe Seq(1)
    sums(group) shouldBe Map(1 -> Some(9L))
  }

  "determinism" should "repeat the same partial at one thread and at four" in {
    val rows = (0 until 40).map { index =>
      val value = if (index % 5 == 0) None else Some(index)
      (Some(index % 7), value)
    }
    val batch = BatchCodec.nullableIntPairs(rows)
    val first = run(Vector(batch), parallelism = 1, batchSize = 8)
    (0 until 16).foreach { _ =>
      samePartial(run(Vector(batch), parallelism = 1, batchSize = 8), first)
      samePartial(run(Vector(batch), parallelism = 4, batchSize = 8), first)
    }
  }

  private def generatedPartial(random: Random): ColumnBatch =
    val rows = (0 until 1 + random.nextInt(24)).map { _ =>
      val key = Some(random.nextInt(12) - 2)
      val value = if (random.nextInt(4) == 0) None else Some(random.nextInt(9) - 3)
      (key, value)
    }
    partial(rows)

  private def block(start: Int, count: Int): ColumnBatch =
    partial((start until start + count).map(key => (Some(key), Some(1))))

  private def partial(rows: Seq[(Option[Int], Option[Int])]): ColumnBatch =
    HashAggregate.sumInt32(pairs(rows), keyOrdinal = 0, valueOrdinal = 1)

  private def pairs(rows: Seq[(Option[Int], Option[Int])]): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.fromNullable(rows.map(_._1)),
        Int32Column.fromNullable(rows.map(_._2)),
      ),
      rows.size,
    )

  private def run(
      batches: Vector[ColumnBatch],
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    GroupedSumKernel.run(
      batches,
      keyOrdinal = 0,
      valueOrdinal = 1,
      numPartitions = 1,
      parallelism = parallelism,
      batchSize = batchSize,
      steps = Vector.empty[PairStep],
    )

  private def one(batches: Vector[ColumnBatch]): ColumnBatch = batches match
    case Vector(batch) => batch
    case other => fail(s"expected one batch, got ${other.length}")

  private def samePartial(left: Vector[ColumnBatch], right: Vector[ColumnBatch]): Unit =
    left.length shouldBe right.length
    left.zip(right).foreach { (got, expected) =>
      keysOf(got) shouldBe keysOf(expected)
      sums(got) shouldBe sums(expected)
    }

  private def fullyPresent(batch: ColumnBatch): Boolean =
    ColumnHash.fullyPresent(int32(batch, 0)) && ColumnHash.fullyPresent(int64(batch, 1))

  private def sums(batch: ColumnBatch): Map[Int, Option[Long]] =
    val keys = int32(batch, 0)
    val values = int64(batch, 1)
    (0 until batch.length).map { row =>
      val value = if (values.isValid(row)) Some(values.values(row)) else None
      keys.values(row) -> value
    }.toMap

  private def keysOf(batch: ColumnBatch): Seq[Int] =
    val keys = int32(batch, 0)
    (0 until batch.length).map(row => keys.values(row))

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int64Column) => column
      case other => fail(s"expected Int64 at $ordinal, got $other")
