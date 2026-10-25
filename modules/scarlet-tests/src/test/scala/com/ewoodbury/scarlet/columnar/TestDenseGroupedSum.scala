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

  "budget" should "derive the cap from 4 MiB of 17-byte slots" in {
    DenseSum.BytesPerSlot shouldBe 17L
    DenseSum.BudgetBytes shouldBe 4L * 1024L * 1024L
    DenseSum.DensityFactor shouldBe 4
    DenseSum.cap(1) shouldBe (DenseSum.BudgetBytes / DenseSum.BytesPerSlot).toInt
    DenseSum.cap(4) shouldBe (DenseSum.BudgetBytes / (DenseSum.BytesPerSlot * 4L)).toInt
    (DenseSum.cap(1).toLong * DenseSum.BytesPerSlot) should be <= DenseSum.BudgetBytes
    (DenseSum.cap(4).toLong * 4L * DenseSum.BytesPerSlot) should be <= DenseSum.BudgetBytes
    (4L * DenseSum.DirectPrefix.toLong * DenseSum.BytesPerSlot) should be > DenseSum.BudgetBytes
    DenseSum.cap(4) should be < DenseSum.DirectPrefix
    DenseSum.cap(1) should be > DenseSum.cap(4)
    DenseSum.DirectPrefix shouldBe 65536
    DenseSum.directLimit(DenseSum.cap(1)) shouldBe DenseSum.DirectPrefix
    DenseSum.directLimit(DenseSum.cap(4)) shouldBe DenseSum.cap(4)
    DenseSum.directLimit(3) shouldBe 3
    val workers = (DenseSum.BudgetBytes / DenseSum.BytesPerSlot).toInt + 1
    DenseSum.cap(workers) shouldBe 0
  }

  it should "size a dense window to the observed keys" in {
    val stored = Array.tabulate(64)(row => row % 16)
    val keys = Int32Column.of(stored, Validity.allValid(stored.length), stored.length)

    DenseSum.classify(keys, start = 0, length = stored.length, DenseSum.cap(1)) shouldBe
      DenseSum.Window(0, 16)
  }

  it should "hash a sparse span that still fits the byte cap" in {
    val stored = Array(0, 300000)
    val keys = Int32Column.of(stored, Validity.allValid(stored.length), stored.length)

    DenseSum.classify(keys, start = 0, length = stored.length, cap = 1000000) shouldBe
      DenseSum.Wide
    DenseSum.classify(keys, start = 0, length = stored.length, cap = 0) shouldBe DenseSum.Wide
  }

  "cap" should "index a window that fits and hash a window one past it" in {
    val cap = 3
    val indexed = cappedPartial(Seq((2, 3), (0, 4)), cap)
    val hashed = cappedPartial(Seq((3, 3), (0, 4)), cap)

    keysOf(indexed) shouldBe Seq(0, 2)
    sums(Vector(indexed)) shouldBe Map(0 -> Some(4L), 2 -> Some(3L))
    keysOf(hashed) shouldBe Seq(3, 0)
    sums(Vector(hashed)) shouldBe Map(0 -> Some(4L), 3 -> Some(3L))
  }

  it should "hash when the budget leaves no dense slot" in {
    val rows = Seq((2, 3), (0, 4))
    val partial = cappedPartial(rows, denseCap = 0)

    keysOf(partial) shouldBe Seq(2, 0)
    sums(Vector(partial)) shouldBe Map(0 -> Some(4L), 2 -> Some(3L))
  }

  it should "hash 1024 groups spread over 0 until 300000" in {
    val distinct = (0 until 1024).map(slot => ((slot.toLong * 300000L) / 1023L).toInt)
    val rows = distinct.reverse.map(key => (key, 1))
    val partial = cappedPartial(rows, denseCap = 1000000)

    keysOf(partial) shouldBe rows.map(_._1)
    keysOf(partial).headOption shouldBe Some(300000)
  }

  it should "hash a sparse window when the next morsel starts higher" in {
    val even = (0 until 8).map(slot => ((slot.toLong * 300000L) / 7L).toInt)
    val odd = even.map(key => key + 7)
    val rows = even.reverse.map(key => (key, 1)) ++ odd.reverse.map(key => (key, 1))
    val morsels = partials(BatchCodec.intPairs(rows), batchSize = even.length)

    morsels match
      case Vector(first, second) =>
        keysOf(first).headOption shouldBe even.lastOption
        keysOf(second).headOption shouldBe odd.lastOption
        keysOf(first) shouldBe even.reverse
        keysOf(second) shouldBe odd.reverse
      case other => fail(s"expected two partials, got ${other.length}")
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

  it should "reject a wrapped index instead of summing the wrong slot" in {
    val table = new DenseSum.Accumulator
    table.prepare(min = Int.MaxValue, range = 2)
    val batch = BatchCodec.intPairs(Seq((Int.MaxValue, 4), (Int.MinValue, 9)))

    val thrown = intercept[IllegalStateException] {
      table.addTrusted(int32(batch, 0), int32(batch, 1), start = 0, end = batch.length)
    }
    thrown.getMessage should include("dense window")
    sums(Vector(table.compact())) shouldBe Map(Int.MaxValue -> Some(4L))
  }

  it should "drop sums from a direct block that rejects its last key" in {
    val table = new DenseSum.Accumulator
    val limit = 8
    val failed = BatchCodec.intPairs(
      Seq((0, 10), (1, 20), (2, 30), (3, 40), (4, 50), (5, 60), (6, 70), (limit, 80)),
    )
    val follow = BatchCodec.intPairs(Seq((0, 1), (1, 1), (6, 1)))
    table.prepareDirect(limit)
    table.addDirect(int32(failed, 0), int32(failed, 1), start = 0, end = failed.length) shouldBe
      false
    table.abandonDirect()
    table.prepareDirect(limit)
    table.addDirect(int32(follow, 0), int32(follow, 1), start = 0, end = follow.length) shouldBe
      true

    sums(Vector(table.compactDirect())) shouldBe Map(0 -> Some(1L), 1 -> Some(1L), 6 -> Some(1L))
  }

  it should "write nothing when a later key falls outside the prefix" in {
    val table = new DenseSum.Accumulator
    val limit = 8
    val failed = BatchCodec.intPairs((0 until limit).map(key => (key, 10 + key)) :+ (limit, 80))
    val follow = BatchCodec.intPairs(Seq((0, 1), (3, 1)))
    table.prepareDirect(limit)
    table.addDirect(int32(failed, 0), int32(failed, 1), start = 0, end = failed.length) shouldBe
      false
    table.abandonDirect()
    table.prepareDirect(limit)
    table.addDirect(int32(follow, 0), int32(follow, 1), start = 0, end = follow.length) shouldBe
      true

    sums(Vector(table.compactDirect())) shouldBe Map(0 -> Some(1L), 3 -> Some(1L))
  }

  it should "clear slots written before a direct scan fails" in {
    val table = new DenseSum.Accumulator
    val limit = 8
    val failed = BatchCodec.intPairs(Seq((0, 10), (1, 20), (2, 30), (3, 40)))
    val follow = BatchCodec.intPairs(Seq((1, 5), (4, 7)))
    table.prepareDirect(limit)
    table.failAfter(2)
    intercept[ArrayIndexOutOfBoundsException] {
      val _ = table.addDirect(int32(failed, 0), int32(failed, 1), start = 0, end = failed.length)
    }

    table.touchedSlots should be > 0
    table.prepareDirect(limit)
    table.touchedSlots shouldBe 0
    table.addDirect(int32(follow, 0), int32(follow, 1), start = 0, end = follow.length) shouldBe
      true
    sums(Vector(table.compactDirect())) shouldBe Map(1 -> Some(5L), 4 -> Some(7L))
  }

  it should "drop a failed worker scan before the same window is reused" in {
    val failed = BatchCodec.intPairs((0 until 8).map(key => (key, 10 + key)))
    val follow = BatchCodec.intPairs(Seq((7, 1), (0, 2), (3, 4)))
    HashAggregate.armDirectFailure(2)
    try
      intercept[ArrayIndexOutOfBoundsException] {
        val _ = HashAggregate.indexSumRange(
          int32(failed, 0),
          int32(failed, 1),
          start = 0,
          length = failed.length,
          minKey = 0,
          keyRange = 8,
          columnsPresent = true,
        )
      }
      HashAggregate.directTouchedSlots shouldBe 0
      val batch = HashAggregate.indexSumRange(
        int32(follow, 0),
        int32(follow, 1),
        start = 0,
        length = follow.length,
        minKey = 0,
        keyRange = 8,
        columnsPresent = true,
      )
      sums(Vector(batch)) shouldBe Map(0 -> Some(2L), 3 -> Some(4L), 7 -> Some(1L))
    finally HashAggregate.releaseSumWorkers()
  }

  it should "add the window origin when a gapped range is compacted" in {
    val table = new DenseSum.Accumulator
    val shifted = BatchCodec.intPairs(Seq((105, 4), (100, 1), (103, 2)))
    table.prepare(min = 100, range = 6)
    table.addTrusted(int32(shifted, 0), int32(shifted, 1), start = 0, end = shifted.length)

    keysOf(table.compact()) shouldBe Seq(100, 103, 105)
    table.prepare(min = 200, range = 6)
    table.touchedSlots shouldBe 0
    val next = BatchCodec.intPairs(Seq((204, 3), (200, 8)))
    table.addTrusted(int32(next, 0), int32(next, 1), start = 0, end = next.length)
    sums(Vector(table.compact())) shouldBe Map(200 -> Some(8L), 204 -> Some(3L))
  }

  it should "reject a direct scan whose values are not all present" in {
    val stored = Array(0, 1)
    val keys = Int32Column.of(stored, Validity.allValid(stored.length), stored.length)
    val values = Int32Column.of(stored, Validity.pack(stored.length, row => row == 0), stored.length)
    val table = new DenseSum.Accumulator
    table.prepareDirect(4)

    intercept[AssertionError] {
      val _ = table.addDirect(keys, values, start = 0, end = stored.length)
    }
    table.touchedSlots shouldBe 0
  }

  "failure" should "not carry a rejected direct scan into the next morsel" in {
    val wide = DenseSum.cap(1)
    val failed = Seq((0, 10), (1, 20), (2, 30), (3, 40), (4, 50), (5, 60), (6, 70), (wide, 80))
    val follow = Seq((0, 1), (1, 1), (6, 1))
    val batch = BatchCodec.intPairs(failed ++ follow)
    val morsels = partials(batch, batchSize = failed.length)
    val expected = sums(Vector(HashAggregate.sumInt32(batch, 0, 1)))

    val failedSum = sums(Vector(HashAggregate.sumInt32(BatchCodec.intPairs(failed), 0, 1)))
    val followSum = sums(Vector(HashAggregate.sumInt32(BatchCodec.intPairs(follow), 0, 1)))
    morsels match
      case Vector(first, second) =>
        sums(Vector(first)) shouldBe failedSum
        sums(Vector(second)) shouldBe followSum
      case other => fail(s"expected two partials, got ${other.length}")
    sums(plain(Vector(batch), parallelism = 1, batchSize = failed.length)) shouldBe expected
  }

  "publish" should "give each morsel its own key array and validity" in {
    val rows = Seq((0, 1), (1, 2), (2, 3), (0, 4), (1, 5), (2, 6))
    partials(BatchCodec.intPairs(rows), batchSize = 3) match
      case Vector(first, second) =>
        val leftKeys = int32(first, 0)
        val rightKeys = int32(second, 0)
        val leftSums = int64(first, 1)
        val rightSums = int64(second, 1)
        leftKeys.values should not be theSameInstanceAs(rightKeys.values)
        leftKeys.validity should not be theSameInstanceAs(rightKeys.validity)
        leftSums.validity should not be theSameInstanceAs(rightSums.validity)
        leftKeys.validity should not be theSameInstanceAs(leftSums.validity)
        rightKeys.validity should not be theSameInstanceAs(rightSums.validity)
        leftKeys.validity should not be theSameInstanceAs(rightSums.validity)
        leftSums.values should not be theSameInstanceAs(rightSums.values)
      case other => fail(s"expected two partials, got ${other.length}")
  }

  "threads" should "repeat dense sums at four threads" in {
    val rows = (0 until 128).map(row => (row % 16, row % 7))
    val batch = BatchCodec.intPairs(rows)
    val expected = sums(Vector(HashAggregate.sumInt32(batch, 0, 1)))
    val runs = (0 until 8).map { _ =>
      sums(plain(Vector(batch), parallelism = 4, batchSize = 8))
    }

    runs.foreach(_ shouldBe expected)
  }

  it should "match the hash table when dense and wide morsels share a batch" in {
    val rows = Seq(
      (1, 4),
      (4, 1),
      (1, 2),
      (8, 3),
      (1, 5),
      (Int.MaxValue, 7),
      (4, 1),
      (0, 2),
      (8, 1),
      (0, 6),
      (2, 2),
      (2, 2),
    )
    val batch = BatchCodec.intPairs(rows)
    val expected = sums(Vector(HashAggregate.sumInt32(batch, 0, 1)))
    val runs = (0 until 8).map { _ =>
      sums(plain(Vector(batch), parallelism = 4, batchSize = 4))
    }

    runs.foreach(_ shouldBe expected)
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

  private def cappedPartial(rows: Seq[(Int, Int)], denseCap: Int): ColumnBatch =
    val batch = BatchCodec.intPairs(rows)
    val (sizes, morsels) = GroupedSumKernel.partialMorsels(
      Vector(batch),
      keyOrdinal = 0,
      valueOrdinal = 1,
      parallelism = 1,
      batchSize = math.max(batch.length, 1),
      steps = Vector.empty[PairStep],
      denseCap = denseCap,
    )
    sizes shouldBe Vector(1)
    morsels match
      case Vector(only) => only
      case other => fail(s"expected one partial, got ${other.length}")

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
