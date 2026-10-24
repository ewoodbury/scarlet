package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Direct-index grouped sum against the hash fallback.
 *
 * The index emits ascending keys. The hash table emits first-seen order. These tests compare
 * `Map[Int, Option[Long]]`, and they read key order only to tell the two paths apart.
 */
class TestDenseGroupedSum extends AnyFlatSpec with Matchers:

  "classification" should "reject the full Int domain and accept a subrange" in {
    val keys = Int32Column.of(
      Array(Int.MinValue, 5, Int.MaxValue),
      Validity.allValid(3),
      3,
    )
    val cap = DenseSum.cap(1)

    DenseSum.classify(keys, start = 0, length = 3, cap) shouldBe DenseSum.Wide
    DenseSum.classify(keys, start = 1, length = 1, cap) shouldBe DenseSum.Window(5, 1)
    DenseSum.classify(keys, start = 0, length = 0, cap) shouldBe DenseSum.Empty
  }

  it should "skip null keys when measuring the range" in {
    val stored = Array(Int.MinValue, -2, 3)
    val present = Validity.pack(stored.length, row => row != 0)
    val keys = Int32Column.of(stored, present, stored.length)

    DenseSum.classify(keys, start = 0, length = keys.length, DenseSum.cap(1)) shouldBe
      DenseSum.Window(-2, 6)
  }

  it should "ignore keys outside the scanned rows" in {
    val keys = Int32Column.of(Array(0, 1, 1000000000), Validity.allValid(3), 3)

    DenseSum.classify(keys, start = 0, length = 2, DenseSum.cap(4)) shouldBe DenseSum.Window(0, 2)
    DenseSum.classify(keys, start = 0, length = 3, DenseSum.cap(4)) shouldBe DenseSum.Wide
  }

  "budget" should "derive the cap from 4 MiB of 9-byte slots" in {
    DenseSum.BytesPerSlot shouldBe 9L
    DenseSum.BudgetBytes shouldBe 4L * 1024L * 1024L
    DenseSum.cap(1) shouldBe (DenseSum.BudgetBytes / DenseSum.BytesPerSlot).toInt
    DenseSum.cap(4) shouldBe (DenseSum.BudgetBytes / (DenseSum.BytesPerSlot * 4L)).toInt
    (4L * 65536L * DenseSum.BytesPerSlot) should be < DenseSum.BudgetBytes
    DenseSum.cap(4) should be >= 65536
    DenseSum.DirectPrefix shouldBe 65536
    DenseSum.directLimit(DenseSum.cap(1)) shouldBe DenseSum.DirectPrefix
    DenseSum.directLimit(DenseSum.cap(4)) shouldBe DenseSum.DirectPrefix
    DenseSum.directLimit(3) shouldBe 3
  }

  "cap" should "index a range of denseCap and hash a range one past it" in {
    val cap = DenseSum.cap(4)
    val atCap = onePartial(BatchCodec.intPairs(Seq((cap - 1, 3), (0, 4))), parallelism = 4)
    val overAtFour = onePartial(BatchCodec.intPairs(Seq((cap, 3), (0, 4))), parallelism = 4)
    val overAtOne = onePartial(BatchCodec.intPairs(Seq((cap, 3), (0, 4))), parallelism = 1)

    keysOf(atCap) shouldBe Seq(0, cap - 1)
    sums(Vector(atCap)) shouldBe Map(0 -> Some(4L), cap - 1 -> Some(3L))
    keysOf(overAtFour) shouldBe Seq(cap, 0)
    keysOf(overAtOne) shouldBe Seq(0, cap)
  }

  "sums" should "match the hash table, including a null sum and a rejected key" in {
    val rows = Seq(
      (Some(1), Some(5)),
      (Some(1), None),
      (Some(2), None),
      (Some(2), None),
      (Some(4), Some(-3)),
      (Some(4), Some(-1)),
      (None, Some(9)),
      (Some(8), Some(2)),
    )
    val batch = BatchCodec.nullableIntPairs(rows)
    val keep: ColumnKernel.Int32Predicate = value => value > 0
    val map: ColumnKernel.Int32Map = value => value

    sums(plain(Vector(batch), parallelism = 4, batchSize = 2)) shouldBe sums(
      Vector(HashAggregate.sumInt32(batch, 0, 1)),
    )
    sums(plain(Vector(batch), parallelism = 4, batchSize = 2)) shouldBe Map(
      1 -> Some(5L),
      2 -> None,
      4 -> Some(-4L),
      8 -> Some(2L),
    )
    sums(filtered(Vector(batch), parallelism = 1, batchSize = 3, keep, map)) shouldBe sums(
      Vector(HashAggregate.sumInt32(batch, 0, 1, keep, map)),
    )
    sums(filtered(Vector(batch), parallelism = 1, batchSize = 3, keep, map)) shouldBe Map(
      1 -> Some(5L),
      8 -> Some(2L),
    )
  }

  it should "sum negative keys and a window that does not start at zero" in {
    val negative = BatchCodec.intPairs(Seq((-8, 3), (-2, 1), (-8, 4), (0, 5)))
    val offset = BatchCodec.intPairs(Seq((100, 1), (105, 4), (100, 2), (103, 1)))

    sums(plain(Vector(negative), parallelism = 4, batchSize = 1)) shouldBe sums(
      Vector(HashAggregate.sumInt32(negative, 0, 1)),
    )
    sums(plain(Vector(negative), parallelism = 4, batchSize = 1)) shouldBe Map(
      -8 -> Some(7L),
      -2 -> Some(1L),
      0 -> Some(5L),
    )
    sums(plain(Vector(offset), parallelism = 1, batchSize = 8)) shouldBe Map(
      100 -> Some(3L),
      103 -> Some(1L),
      105 -> Some(4L),
    )
    keysOf(onePartial(offset, parallelism = 1)) shouldBe Seq(100, 103, 105)
  }

  it should "hash Int.MinValue together with Int.MaxValue" in {
    val batch = BatchCodec.intPairs(Seq((Int.MaxValue, 2), (Int.MinValue, 5), (Int.MaxValue, 1)))
    val direct = HashAggregate.sumInt32(batch, 0, 1)
    val partial = onePartial(batch, parallelism = 1)

    sums(Vector(partial)) shouldBe sums(Vector(direct))
    keysOf(partial) shouldBe keysOf(direct)
    keysOf(partial) shouldBe Seq(Int.MaxValue, Int.MinValue)
  }

  "steps" should "keep MapPair on the hash path" in {
    val rows = Seq((5, 1), (1, 2), (3, 4))
    val batch = BatchCodec.intPairs(rows)
    val steps = Vector(PairStep.MapPair((key, value) => (key + 10, value)))
    val direct = HashAggregate.sumInt32(batch, 0, 1, steps)
    val partial = onePartial(batch, parallelism = 1, steps)

    sums(Vector(partial)) shouldBe sums(Vector(direct))
    keysOf(partial) shouldBe Seq(15, 11, 13)
    keysOf(partial) shouldBe keysOf(direct)
  }

  it should "index a value filter and hash a pair map through the pipeline" in {
    val rows = Seq((5, 1), (1, 2), (3, 4))
    val batch = Vector(BatchCodec.intPairs(rows))
    val kept = ColumnPipeline.sumByKey(
      batch,
      keyOrdinal = 0,
      valueOrdinal = 1,
      numPartitions = 1,
      parallelism = 1,
      batchSize = 8,
      Vector(PairStep.KeepValue((_: Int) => true)),
    )
    val mapped = ColumnPipeline.sumByKey(
      batch,
      keyOrdinal = 0,
      valueOrdinal = 1,
      numPartitions = 1,
      parallelism = 1,
      batchSize = 8,
      Vector(PairStep.MapPair((key, value) => (key, value))),
    )

    keysOf(only(kept)) shouldBe Seq(1, 3, 5)
    keysOf(only(mapped)) shouldBe Seq(5, 1, 3)
    sums(kept) shouldBe Map(1 -> Some(2L), 3 -> Some(4L), 5 -> Some(1L))
    sums(mapped) shouldBe sums(kept)
  }

  "reuse" should "drop the previous range when the next range reuses the index" in {
    val table = new DenseSum.Accumulator
    val first = BatchCodec.intPairs(Seq((1, 4), (4, 1)))
    val second = BatchCodec.intPairs(Seq((7, 9)))
    table.prepare(min = 0, range = 10)
    table.scanPlain(int32(first, 0), int32(first, 1), start = 0, end = first.length)
    sums(Vector(table.compact())) shouldBe Map(1 -> Some(4L), 4 -> Some(1L))

    table.prepare(min = 0, range = 10)
    table.scanPlain(int32(second, 0), int32(second, 1), start = 0, end = second.length)
    sums(Vector(table.compact())) shouldBe Map(7 -> Some(9L))
  }

  it should "omit a key that only the previous morsel saw" in {
    val shared = BatchCodec.intPairs(Seq((0, 1), (4, 10), (9, 3), (0, 2), (9, 4)))
    val disjoint = BatchCodec.intPairs(Seq((1, 10), (1, 1), (8, 7), (9, 4)))

    partials(shared, batchSize = 3) match
      case Vector(first, second) =>
        sums(Vector(first)) shouldBe Map(0 -> Some(1L), 4 -> Some(10L), 9 -> Some(3L))
        sums(Vector(second)) shouldBe Map(0 -> Some(2L), 9 -> Some(4L))
      case other => fail(s"expected two partials, got ${other.length}")

    partials(disjoint, batchSize = 2) match
      case Vector(first, second) =>
        sums(Vector(first)) shouldBe Map(1 -> Some(11L))
        sums(Vector(second)) shouldBe Map(8 -> Some(7L), 9 -> Some(4L))
      case other => fail(s"expected two partials, got ${other.length}")
  }

  it should "fail a key that sits outside the classified window" in {
    val batch = BatchCodec.intPairs(Seq((5, 1)))
    val table = new DenseSum.Accumulator
    table.prepare(min = 0, range = 2)

    val thrown = intercept[IllegalStateException] {
      table.scanPlain(int32(batch, 0), int32(batch, 1), start = 0, end = batch.length)
    }
    thrown.getMessage should include("dense window")
  }

  "ranges" should "sum a single-row morsel and publish an empty partial for a zero-row batch" in {
    val rows = Seq((2, 5), (2, 1), (-3, 4))
    val batch = BatchCodec.intPairs(rows)
    val (sizes, morsels) = GroupedSumKernel.partialMorsels(
      Vector(batch),
      keyOrdinal = 0,
      valueOrdinal = 1,
      parallelism = 1,
      batchSize = 1,
      steps = Vector.empty[PairStep],
    )
    val empty = BatchCodec.intPairs(Seq.empty[(Int, Int)])
    val (emptySizes, emptyPartials) = GroupedSumKernel.partialMorsels(
      Vector(empty),
      keyOrdinal = 0,
      valueOrdinal = 1,
      parallelism = 4,
      batchSize = 8,
      steps = Vector.empty[PairStep],
    )

    sizes shouldBe Vector(3)
    morsels match
      case Vector(first, second, third) =>
        sums(Vector(first)) shouldBe Map(2 -> Some(5L))
        sums(Vector(second)) shouldBe Map(2 -> Some(1L))
        sums(Vector(third)) shouldBe Map(-3 -> Some(4L))
      case other => fail(s"expected three partials, got ${other.length}")
    sums(plain(Vector(batch), parallelism = 4, batchSize = 1)) shouldBe Map(
      -3 -> Some(4L),
      2 -> Some(6L),
    )
    emptySizes shouldBe Vector(1)
    emptyPartials match
      case Vector(only) =>
        only.length shouldBe 0
        only.schema shouldBe Vector(LogicalType.Int32, LogicalType.Int64)
      case other => fail(s"expected one partial, got ${other.length}")
  }

  it should "count one visit per morsel when the index is used" in {
    val batch = BatchCodec.intPairs((0 until 10).map(row => (row % 3, 1)))
    ColumnMetrics.withSink(new CountingSink) {
      val (sizes, morsels) = GroupedSumKernel.partialMorsels(
        Vector(batch),
        keyOrdinal = 0,
        valueOrdinal = 1,
        parallelism = 4,
        batchSize = 4,
        steps = Vector.empty[PairStep],
      )

      sizes shouldBe Vector(3)
      morsels.length shouldBe 3
      ColumnMetrics.visitCount shouldBe 3L
      ColumnMetrics.batchCount shouldBe morsels.length.toLong
    }
  }

  private def plain(
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

  private def filtered(
      batches: Vector[ColumnBatch],
      parallelism: Int,
      batchSize: Int,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): Vector[ColumnBatch] =
    GroupedSumKernel.run(
      batches,
      keyOrdinal = 0,
      valueOrdinal = 1,
      numPartitions = 1,
      parallelism = parallelism,
      batchSize = batchSize,
      keep,
      map,
    )

  private def onePartial(
      batch: ColumnBatch,
      parallelism: Int,
      steps: Vector[PairStep] = Vector.empty[PairStep],
  ): ColumnBatch =
    val (sizes, morsels) = GroupedSumKernel.partialMorsels(
      Vector(batch),
      keyOrdinal = 0,
      valueOrdinal = 1,
      parallelism = parallelism,
      batchSize = math.max(batch.length, 1),
      steps = steps,
    )
    sizes shouldBe Vector(1)
    morsels match
      case Vector(only) => only
      case other => fail(s"expected one partial, got ${other.length}")

  private def partials(batch: ColumnBatch, batchSize: Int): Vector[ColumnBatch] =
    val (_, morsels) = GroupedSumKernel.partialMorsels(
      Vector(batch),
      keyOrdinal = 0,
      valueOrdinal = 1,
      parallelism = 1,
      batchSize = batchSize,
      steps = Vector.empty[PairStep],
    )
    morsels

  private def only(batches: Vector[ColumnBatch]): ColumnBatch =
    batches match
      case Vector(batch) => batch
      case other => fail(s"expected one batch, got ${other.length}")

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
