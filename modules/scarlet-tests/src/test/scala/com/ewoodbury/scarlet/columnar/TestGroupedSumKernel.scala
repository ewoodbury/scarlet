package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestGroupedSumKernel extends AnyFlatSpec with Matchers:

  private val trials = 128

  "lost updates" should "sum one key split across morsels to 36 on several threads" in {
    val batches = Vector(oneKey)
    (0 until trials).foreach { _ =>
      sums(run(batches, parallelism = 4, batchSize = 1)) shouldBe Map(1 -> Some(36L))
    }
  }

  it should "sum one key split across morsels to 36 on one thread" in {
    val batches = Vector(oneKey)
    (0 until trials).foreach { _ =>
      sums(run(batches, parallelism = 1, batchSize = 1)) shouldBe Map(1 -> Some(36L))
    }
  }

  "reuse" should "omit the previous morsel's groups from the next partial" in {
    val batch = BatchCodec.intPairs(Seq((1, 10), (1, 1), (2, 7), (3, 4)))
    val (sizes, partials) = GroupedSumKernel.partialMorsels(
      Vector(batch),
      keyOrdinal = 0,
      valueOrdinal = 1,
      parallelism = 1,
      batchSize = 2,
      steps = Vector.empty[PairStep],
    )

    sizes shouldBe Vector(2)
    partials match
      case Vector(first, second) =>
        sums(Vector(first)) shouldBe Map(1 -> Some(11L))
        sums(Vector(second)) shouldBe Map(2 -> Some(7L), 3 -> Some(4L))
      case other => fail(s"expected two partials, got ${other.length}")
  }

  it should "omit an all-null group when the next morsel reuses the table on one thread" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq(
        (Some(1), None),
        (Some(1), None),
        (Some(4), Some(8)),
        (Some(5), None),
      ),
    )
    val (sizes, partials) = GroupedSumKernel.partialMorsels(
      Vector(batch),
      keyOrdinal = 0,
      valueOrdinal = 1,
      parallelism = 1,
      batchSize = 2,
      steps = Vector.empty[PairStep],
    )

    sizes shouldBe Vector(2)
    partials match
      case Vector(first, second) =>
        sums(Vector(first)) shouldBe Map(1 -> None)
        sums(Vector(second)) shouldBe Map(4 -> Some(8L), 5 -> None)
      case other => fail(s"expected two partials, got ${other.length}")
  }

  "grow" should "match first-seen order after the worker table grows" in {
    val rows = (0 until 50000).map(index => (index * 10000, index % 97))
    val batch = BatchCodec.intPairs(rows)
    val direct = HashAggregate.sumInt32(batch, keyOrdinal = 0, valueOrdinal = 1)
    val first = onePartial(batch, parallelism = 4, batchSize = 50000)
    val second = onePartial(batch, parallelism = 1, batchSize = 50000)

    sums(Vector(first)) shouldBe sums(Vector(direct))
    sums(Vector(second)) shouldBe sums(Vector(direct))
    keysOf(first) shouldBe keysOf(direct)
    keysOf(second) shouldBe keysOf(first)
  }

  it should "reuse the grown worker table for the next morsel" in {
    val firstRows = (0 until 20).map(index => (index, index + 1))
    val secondRows = Seq((10000, 4), (10001, 6), (10002, 1))
    val batch = BatchCodec.intPairs(firstRows ++ secondRows)
    val (sizes, partials) = GroupedSumKernel.partialMorsels(
      Vector(batch),
      keyOrdinal = 0,
      valueOrdinal = 1,
      parallelism = 1,
      batchSize = firstRows.length,
      steps = Vector.empty[PairStep],
    )

    sizes shouldBe Vector(2)
    val (first, second) = partials match
      case Vector(left, right) => (left, right)
      case other => fail(s"expected two partials, got ${other.length}")
    val firstDirect = HashAggregate.sumInt32(
      BatchCodec.intPairs(firstRows),
      keyOrdinal = 0,
      valueOrdinal = 1,
    )
    val secondDirect = HashAggregate.sumInt32(
      BatchCodec.intPairs(secondRows),
      keyOrdinal = 0,
      valueOrdinal = 1,
    )
    sums(Vector(first)) shouldBe sums(Vector(firstDirect))
    sums(Vector(second)) shouldBe sums(Vector(secondDirect))
    keysOf(first) shouldBe keysOf(firstDirect)
    keysOf(second) shouldBe keysOf(secondDirect)

    // The action dropped the grown table, so the next scan starts at the initial capacity.
    try
      val restarted = scanRange(Seq((7, 1)))
      HashAggregate.sumWorkerCapacity shouldBe HashAggregate.SumTableStart
      sums(Vector(restarted)) shouldBe Map(7 -> Some(1L))
    finally HashAggregate.releaseSumWorkers()

    // SumTableStart doubles at 8 live groups and again at 16, so 20 groups sit at 64.
    try
      val grown = scanRange(firstRows)
      val grownCapacity = HashAggregate.sumWorkerCapacity
      val reused = scanRange(secondRows)
      grownCapacity shouldBe 64
      HashAggregate.sumWorkerCapacity shouldBe grownCapacity
      sums(Vector(grown)) shouldBe sums(Vector(first))
      sums(Vector(reused)) shouldBe sums(Vector(second))
      keysOf(grown) shouldBe keysOf(first)
      keysOf(reused) shouldBe keysOf(second)
    finally HashAggregate.releaseSumWorkers()
  }

  "nulls" should "keep a null sum when every value of a key is null" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq(
        (Some(1), None),
        (Some(1), Some(4)),
        (Some(2), None),
        (None, Some(9)),
        (Some(2), None),
      ),
    )

    sums(run(Vector(batch), parallelism = 4, batchSize = 1)) shouldBe Map(
      1 -> Some(4L),
      2 -> None,
    )
  }

  it should "drop a null value and a rejected key when the sum filters" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq(
        (Some(1), Some(5)),
        (Some(1), None),
        (Some(1), Some(-2)),
        (Some(2), None),
        (Some(3), Some(-4)),
        (None, Some(9)),
      ),
    )
    val filtered = ColumnPipeline.sumByKey(
      Vector(batch),
      keyOrdinal = 0,
      valueOrdinal = 1,
      numPartitions = 1,
      parallelism = 4,
      batchSize = 1,
      (value: Int) => value > 0,
      (value: Int) => value * 10,
    )
    val kept = GroupedSumKernel.run(
      Vector(batch),
      keyOrdinal = 0,
      valueOrdinal = 1,
      numPartitions = 1,
      parallelism = 1,
      batchSize = 1,
      Vector(PairStep.KeepValue((value: Int) => value > 0)),
    )

    sums(filtered) shouldBe Map(1 -> Some(50L))
    sums(kept) shouldBe Map(1 -> Some(5L))
  }

  it should "drop a null value and a rejected key when steps filter" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq(
        (Some(1), Some(5)),
        (Some(1), None),
        (Some(7), Some(9)),
        (Some(4), Some(-3)),
        (Some(4), None),
        (Some(8), None),
      ),
    )
    val steps = Vector(
      PairStep.KeepRow((key, value) => key != 7 && value > 0),
      PairStep.MapPair((key, value) => (key + 1, value)),
    )

    sums(run(Vector(batch), parallelism = 4, batchSize = 1, steps)) shouldBe Map(2 -> Some(5L))
  }

  "ranges" should "publish an empty partial for a zero-row batch" in {
    val empty = BatchCodec.intPairs(Seq.empty[(Int, Int)])
    val keys = Int32Column.of(Array(1, 2), Validity.allValid(2), 2)
    val values = Int32Column.of(Array(3, 4), Validity.allValid(2), 2)
    val (sizes, partials) = GroupedSumKernel.partialMorsels(
      Vector(empty),
      keyOrdinal = 0,
      valueOrdinal = 1,
      parallelism = 4,
      batchSize = 8,
      steps = Vector.empty[PairStep],
    )

    sizes shouldBe Vector(1)
    partials match
      case Vector(only) =>
        only.length shouldBe 0
        only.schema shouldBe Vector(LogicalType.Int32, LogicalType.Int64)
      case other => fail(s"expected one partial, got ${other.length}")
    val emptyRange = HashAggregate.sumRange(keys, values, start = 0, length = 0)
    try emptyRange.length shouldBe 0
    finally HashAggregate.releaseSumWorkers()
  }

  it should "return the partial of a batch that fits in one morsel" in {
    val rows = Seq((4, 3), (1, 5), (4, 1))
    val batch = BatchCodec.intPairs(rows)
    val direct = HashAggregate.sumInt32(batch, keyOrdinal = 0, valueOrdinal = 1)
    val partial = onePartial(batch, parallelism = 4, batchSize = 8)
    val merged = GroupedSumKernel.run(
      Vector(batch),
      keyOrdinal = 0,
      valueOrdinal = 1,
      numPartitions = 4,
      parallelism = 4,
      batchSize = 8,
      steps = Vector.empty[PairStep],
    )

    merged match
      case Vector(only) =>
        // Keys arrive as 4 then 1. The direct index emits ascending order. The hash aggregate
        // still emits first-seen order. Grouped sums are compared as maps.
        keysOf(direct) shouldBe Seq(4, 1)
        keysOf(partial) shouldBe Seq(1, 4)
        keysOf(only) shouldBe Seq(1, 4)
        sums(merged) shouldBe sums(Vector(direct))
      case other => fail(s"expected one merged batch, got ${other.length}")
  }

  it should "sum when more workers are requested than morsels" in {
    val rows = Seq((1, 1), (1, 2), (2, 3), (2, 4))
    val summed = run(Vector(BatchCodec.intPairs(rows)), parallelism = 8, batchSize = 2)

    sums(summed) shouldBe Map(1 -> Some(3L), 2 -> Some(7L))
  }

  it should "return one bucket when there is one partition" in {
    val rows = Seq((1, 4), (2, 5), (1, 1), (3, 8))
    val parts = ColumnPipeline.sumByKey(
      Vector(BatchCodec.intPairs(rows)),
      keyOrdinal = 0,
      valueOrdinal = 1,
      numPartitions = 1,
      parallelism = 4,
      batchSize = 2,
    )

    parts.length shouldBe 1
    sums(parts) shouldBe Map(1 -> Some(5L), 2 -> Some(5L), 3 -> Some(8L))
  }

  it should "count one visit and one partial batch per morsel" in {
    val batch = BatchCodec.intPairs((0 until 10).map(row => (row, 1)))
    ColumnMetrics.withSink(new CountingSink) {
      val (sizes, partials) = GroupedSumKernel.partialMorsels(
        Vector(batch),
        keyOrdinal = 0,
        valueOrdinal = 1,
        parallelism = 4,
        batchSize = 4,
        steps = Vector.empty[PairStep],
      )

      sizes shouldBe Vector(3)
      partials.length shouldBe 3
      ColumnMetrics.visitCount shouldBe 3L
      ColumnMetrics.batchCount shouldBe partials.length.toLong
    }
  }

  "partitions" should "reject a non-positive partition count" in {
    val batches = Vector(BatchCodec.intPairs(Seq((1, 1))))

    val zero = intercept[IllegalArgumentException] {
      ColumnPipeline.sumByKey(
        batches,
        keyOrdinal = 0,
        valueOrdinal = 1,
        numPartitions = 0,
        parallelism = 1,
        batchSize = 8,
      )
    }
    val negative = intercept[IllegalArgumentException] {
      GroupedSumKernel.run(
        batches,
        keyOrdinal = 0,
        valueOrdinal = 1,
        numPartitions = -1,
        parallelism = 1,
        batchSize = 8,
        steps = Vector.empty[PairStep],
      )
    }

    zero.getMessage should include("numPartitions")
    negative.getMessage should include("numPartitions")
  }

  it should "return one empty batch per partition for an empty input" in {
    val parts = ColumnPipeline.sumByKey(
      Vector.empty[ColumnBatch],
      keyOrdinal = 0,
      valueOrdinal = 1,
      numPartitions = 4,
      parallelism = 2,
      batchSize = 8,
    )
    val direct = GroupedSumKernel.run(
      Vector.empty[ColumnBatch],
      keyOrdinal = 0,
      valueOrdinal = 1,
      numPartitions = 4,
      parallelism = 2,
      batchSize = 8,
      steps = Vector.empty[PairStep],
    )

    parts.length shouldBe 4
    parts.map(_.length) shouldBe Seq(0, 0, 0, 0)
    direct.length shouldBe 0
  }

  private def scanRange(rows: Seq[(Int, Int)]): ColumnBatch =
    val batch = BatchCodec.intPairs(rows)
    HashAggregate.sumRange(int32(batch, 0), int32(batch, 1), start = 0, length = batch.length)

  private def oneKey: ColumnBatch =
    BatchCodec.intPairs((1 to 8).map(value => (1, value)))

  private def run(
      batches: Vector[ColumnBatch],
      parallelism: Int,
      batchSize: Int,
      steps: Vector[PairStep] = Vector.empty[PairStep],
  ): Vector[ColumnBatch] =
    ColumnPipeline.sumByKey(
      batches,
      keyOrdinal = 0,
      valueOrdinal = 1,
      numPartitions = 4,
      parallelism = parallelism,
      batchSize = batchSize,
      steps = steps,
    )

  private def onePartial(batch: ColumnBatch, parallelism: Int, batchSize: Int): ColumnBatch =
    val (sizes, partials) = GroupedSumKernel.partialMorsels(
      Vector(batch),
      keyOrdinal = 0,
      valueOrdinal = 1,
      parallelism = parallelism,
      batchSize = batchSize,
      steps = Vector.empty[PairStep],
    )
    sizes shouldBe Vector(1)
    partials match
      case Vector(only) => only
      case other => fail(s"expected one partial, got ${other.length}")

  private def sums(batches: Vector[ColumnBatch]): Map[Int, Option[Long]] =
    batches.flatMap { batch =>
      val keys = int32(batch, 0)
      val values = int64(batch, 1)
      (0 until batch.length).map { row =>
        val value = if (values.isValid(row)) Some(values.values(row)) else None
        keys.values(row) -> value
      }
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
