package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Survivor index for a stack of filters. One compact, and nulls stay off the predicate. */
class TestSelection extends AnyFlatSpec with Matchers:

  "a stack" should "keep a present zero and skip a null on the predicate column" in {
    val values = Int32Column.fromNullable(Seq(Some(1), None, Some(0), Some(-2)))
    val firstCalls = new AtomicInteger
    val secondCalls = new AtomicInteger
    val selected = Selection(
      single(values),
      0,
      values.length,
      Seq(
        Selection.Step.Int32(
          0,
          value => {
            firstCalls.incrementAndGet()
            value >= 0
          },
        ),
        Selection.Step.Int32(
          0,
          value => {
            secondCalls.incrementAndGet()
            value != 1
          },
        ),
      ),
    )

    firstCalls.get() shouldBe 3
    secondCalls.get() shouldBe 2
    BatchCodec.decodeInts(selected) shouldBe Seq(0)
    values.values.toSeq shouldBe Seq(1, 0, 0, -2)
  }

  it should "match one ColumnKernel filter and a dense prefix" in {
    val batch = BatchCodec.ints(Seq(1, 0, 2, -1))
    val predicate: ColumnKernel.Int32Predicate = value => value >= 0
    val selected = Selection(batch, 0, batch.length, Seq(Selection.Step.Int32(0, predicate)))
    val kernel = ColumnKernel.filterInt32(batch, 0, predicate)

    BatchCodec.decodeInts(selected) shouldBe Seq(1, 0, 2)
    sameInt(selected, kernel)
    val column = int32(selected, 0)
    (0 until selected.length).foreach(row => column.isValid(row) shouldBe true)
  }

  it should "publish one batch where two filters publish two" in {
    val batch = BatchCodec.ints(Seq(1, 2, 3, 4))
    val steps = Seq(
      Selection.Step.Int32(0, (value: Int) => value > 1),
      Selection.Step.Int32(0, (value: Int) => value < 4),
    )
    ColumnMetrics.withSink(new CountingSink) {
      val selected = Selection(batch, 0, batch.length, steps)
      BatchCodec.decodeInts(selected) shouldBe Seq(2, 3)
      ColumnMetrics.batchCount shouldBe 1L
    }
    ColumnMetrics.withSink(new CountingSink) {
      val first = ColumnKernel.filterInt32(batch, 0, (value: Int) => value > 1)
      val second = ColumnKernel.filterInt32(first, 0, (value: Int) => value < 4)
      BatchCodec.decodeInts(second) shouldBe Seq(2, 3)
      ColumnMetrics.batchCount shouldBe 2L
    }
  }

  it should "read a window by absolute row and leave the outside rows untouched" in {
    val stored = Array(-1, -1, 1, 0, 2, -1)
    val column = Int32Column.of(stored, Validity.allValid(stored.length), stored.length)
    val batch = single(column)
    val selected = Selection(
      batch,
      2,
      3,
      Seq(
        Selection.Step.Int32(
          0,
          value => {
            if (value < 0) then fail("predicate read outside the window")
            value >= 0
          },
        ),
        Selection.Step.Int32(0, (value: Int) => value != 0),
      ),
    )

    BatchCodec.decodeInts(selected) shouldBe Seq(1, 2)
    // Output arrays stay the morsel width. Two survivors do not reallocate a shorter buffer.
    int32(selected, 0).values.length shouldBe 3
    stored.toSeq shouldBe Seq(-1, -1, 1, 0, 2, -1)
  }

  it should "store absolute ids for a window starting at row 2 and materialize those rows" in {
    val stored = Array(-1, -1, 1, 0, 2, -1)
    val column = Int32Column.of(stored, Validity.allValid(stored.length), stored.length)
    val batch = single(column)
    val steps = Seq(
      Selection.Step.Int32(0, (value: Int) => value >= 0),
      Selection.Step.Int32(0, (value: Int) => value != 0),
    )
    val indexed = Selection.applySelected(batch, 2, 3, steps)
    val gathered = Selection.materialize(indexed)
    val direct = Selection(batch, 2, 3, steps)

    indexed.count shouldBe 2
    indexed.rows(0) shouldBe 2
    indexed.rows(1) shouldBe 4
    indexed.rows.length shouldBe 3
    BatchCodec.decodeInts(gathered) shouldBe BatchCodec.decodeInts(direct)
    BatchCodec.decodeInts(gathered) shouldBe Seq(1, 2)
    int32(gathered, 0).values.length shouldBe int32(direct, 0).values.length
    int32(gathered, 0).values.length shouldBe 3
    stored.toSeq shouldBe Seq(-1, -1, 1, 0, 2, -1)
  }

  it should "decode a pair index and a triple index without gathering" in {
    val pairs = BatchCodec.intPairs(Seq((1, 10), (2, 20), (3, 30), (4, 40)))
    val pairIndex = Selection.applySelected(
      pairs,
      0,
      pairs.length,
      Seq(
        Selection.Step.Int32(0, (value: Int) => value > 1),
        Selection.Step.Int32(1, (value: Int) => value < 40),
      ),
    )
    val pairGathered = Selection.materialize(pairIndex)
    val pairPrefix = BatchCodec.decodeIntPairs(pairGathered, 1)
    BatchCodec.decodeIntPairs(pairIndex.batch, pairIndex.rows, pairIndex.count) shouldBe
      BatchCodec.decodeIntPairs(pairGathered)
    ColumnMetrics.withSink(new CountingSink) {
      val prefix = BatchCodec.decodeIntPairs(pairIndex.batch, pairIndex.rows, 1)
      prefix shouldBe pairPrefix
      prefix shouldBe Seq((2, 20))
      ColumnMetrics.batchCount shouldBe 0L
      ColumnMetrics.decodedValueCount shouldBe 1L
    }

    val tripleBatch = triples(
      Array(1, 2, 3, 4),
      Array(10, 20, 30, 40),
      Array(7, 8, -1, 9),
      Array(true, true, true, true),
      Array(true, true, true, true),
      Array(true, true, true, true),
    )
    val tripleIndex = Selection.applySelected(
      tripleBatch,
      0,
      tripleBatch.length,
      Seq(
        Selection.Step.IntTriple(2, (value: Int) => value > 0),
        Selection.Step.IntTriple(0, (value: Int) => value < 4),
      ),
    )
    val tripleGathered = Selection.materialize(tripleIndex)
    val triplePrefix = BatchCodec.decodeIntTriples(tripleGathered, 1)
    BatchCodec.decodeIntTriples(tripleIndex.batch, tripleIndex.rows, tripleIndex.count) shouldBe
      BatchCodec.decodeIntTriples(tripleGathered)
    ColumnMetrics.withSink(new CountingSink) {
      val prefix = BatchCodec.decodeIntTriples(tripleIndex.batch, tripleIndex.rows, 1)
      prefix shouldBe triplePrefix
      prefix shouldBe Seq((1, (10, 7)))
      ColumnMetrics.batchCount shouldBe 0L
      ColumnMetrics.decodedValueCount shouldBe 1L
    }
  }

  it should "skip null longs and doubles and keep a present zero" in {
    val longs = Int64Column.fromNullable(Seq(Some(0L), None, Some(-1L), Some(4L)))
    val longCalls = new AtomicInteger
    val longSelected = Selection(
      single(longs),
      0,
      longs.length,
      Seq(
        Selection.Step.Int64(
          0,
          value => {
            longCalls.incrementAndGet()
            value >= 0L
          },
        ),
        Selection.Step.Int64(0, (value: Long) => value == 0L),
      ),
    )
    longCalls.get() shouldBe 3
    BatchCodec.decodeLongs(longSelected) shouldBe Seq(0L)

    val floats = Float64Column.fromNullable(Seq(Some(0.0), None, Some(-0.0), Some(1.0)))
    val floatCalls = new AtomicInteger
    val floatSelected = Selection(
      single(floats),
      0,
      floats.length,
      Seq(
        Selection.Step.Float64(
          0,
          value => {
            floatCalls.incrementAndGet()
            value == 0.0
          },
        ),
        Selection.Step.Float64(0, (_: Double) => true),
      ),
    )
    floatCalls.get() shouldBe 3
    val decoded = float64(floatSelected)
    decoded.length shouldBe 2
    java.lang.Double.doubleToRawLongBits(decoded.values(0)) shouldBe java.lang.Double
      .doubleToRawLongBits(0.0)
    java.lang.Double.doubleToRawLongBits(decoded.values(1)) shouldBe java.lang.Double
      .doubleToRawLongBits(-0.0)
  }

  it should "skip a null bool, keep a present false, and normalize a set byte" in {
    val bytes = Array[Byte](2, 0, 1)
    val present = Array(true, true, false)
    val column = BoolColumn.of(bytes, Validity.pack(bytes.length, row => present(row)), bytes.length)
    val firstCalls = new AtomicInteger
    val secondCalls = new AtomicInteger
    val selected = Selection(
      single(column),
      0,
      column.length,
      Seq(
        Selection.Step.Bool(
          0,
          value => {
            firstCalls.incrementAndGet()
            value || !value
          },
        ),
        Selection.Step.Bool(
          0,
          value => {
            secondCalls.incrementAndGet()
            value
          },
        ),
      ),
    )

    firstCalls.get() shouldBe 2
    secondCalls.get() shouldBe 2
    BatchCodec.decodeBools(selected) shouldBe Seq(true)
    val out = bool(selected)
    out.values(0) shouldBe 1.toByte
    out.isValid(0) shouldBe true
  }

  it should "keep a null value beside two key filters and drop a null key without calling" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq(
        (Some(1), None),
        (None, Some(9)),
        (Some(4), Some(8)),
        (Some(0), Some(3)),
      ),
    )
    val firstCalls = new AtomicInteger
    val secondCalls = new AtomicInteger
    val selected = Selection(
      batch,
      0,
      batch.length,
      Seq(
        Selection.Step.Int32(
          0,
          value => {
            firstCalls.incrementAndGet()
            value != 4
          },
        ),
        Selection.Step.Int32(
          0,
          value => {
            secondCalls.incrementAndGet()
            value >= 0
          },
        ),
      ),
    )

    firstCalls.get() shouldBe 3
    secondCalls.get() shouldBe 2
    BatchCodec.decodeNullableIntPairs(selected) shouldBe Seq(
      (Some(1), None),
      (Some(0), Some(3)),
    )
    val value = int32(selected, 1)
    value.isValid(0) shouldBe false
    value.values(0) shouldBe 0
    value.isValid(1) shouldBe true
    val key = int32(selected, 0)
    key.isValid(0) shouldBe true
    key.isValid(1) shouldBe true
    int32(batch, 0).values.toSeq shouldBe Seq(1, 0, 4, 0)
  }

  it should "match two chained long key filters and keep a null on the other column" in {
    val batch = new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int64Column.fromNullable(Seq(Some(1L), None, Some(4L), Some(0L))),
        Int64Column.fromNullable(Seq(None, Some(9L), Some(8L), Some(3L))),
      ),
      4,
    )
    val firstCalls = new AtomicInteger
    val secondCalls = new AtomicInteger
    val first: ColumnKernel.Int64Predicate = value => {
      firstCalls.incrementAndGet()
      value != 4L
    }
    val second: ColumnKernel.Int64Predicate = value => {
      secondCalls.incrementAndGet()
      value >= 0L
    }
    val steps = Seq(Selection.Step.Int64(0, first), Selection.Step.Int64(0, second))
    val indexed = Selection.applySelected(batch, 0, batch.length, steps)
    val gathered = Selection.materialize(indexed)
    val kernel = ColumnKernel.filterInt64(ColumnKernel.filterInt64(batch, 0, first), 0, second)

    // The index and the chained kernel share the predicates, so each path contributes its calls.
    firstCalls.get() shouldBe 6
    secondCalls.get() shouldBe 4
    indexed.count shouldBe 2
    indexed.rows(0) shouldBe 0
    indexed.rows(1) shouldBe 3
    sameLongPairs(BatchCodec.decodeLongPairs(gathered), BatchCodec.decodeLongPairs(kernel))
    sameLongPairs(
      BatchCodec.decodeLongPairs(indexed.batch, indexed.rows, indexed.count),
      BatchCodec.decodeLongPairs(gathered),
    )
    val value = int64(gathered, 1)
    value.isValid(0) shouldBe false
    value.values(0) shouldBe 0L
    value.isValid(1) shouldBe true
    value.values(1) shouldBe 3L
    int64(gathered, 0).isValid(0) shouldBe true
    int64(gathered, 0).isValid(1) shouldBe true
    int64(batch, 1).isValid(0) shouldBe false
  }

  it should "keep a null value past a 64-row boundary" in {
    val rows = 70
    val pairs = (0 until rows).map { row =>
      val key = if (row == 65) None else Some(row)
      val value = if (row == 64 || row == 66) None else Some(row * 10)
      (key, value)
    }
    val batch = BatchCodec.nullableIntPairs(pairs)
    val firstCalls = new AtomicInteger
    val secondCalls = new AtomicInteger
    val selected = Selection(
      batch,
      0,
      batch.length,
      Seq(
        Selection.Step.Int32(
          0,
          value => {
            firstCalls.incrementAndGet()
            value % 2 == 0
          },
        ),
        Selection.Step.Int32(
          0,
          value => {
            secondCalls.incrementAndGet()
            value >= 0
          },
        ),
      ),
    )
    val kernel = ColumnKernel.filterInt32(
      ColumnKernel.filterInt32(batch, 0, (value: Int) => value % 2 == 0),
      0,
      (value: Int) => value >= 0,
    )

    firstCalls.get() shouldBe 69
    secondCalls.get() shouldBe 35
    BatchCodec.decodeNullableIntPairs(selected) shouldBe BatchCodec.decodeNullableIntPairs(kernel)
    val decoded = BatchCodec.decodeNullableIntPairs(selected)
    decoded should contain((Some(64), None))
    decoded should contain((Some(66), None))
    decoded.exists(_._1.isEmpty) shouldBe false
  }

  it should "not call a value predicate on a null value that a key filter kept" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq(
        (Some(1), None),
        (None, Some(9)),
        (Some(4), Some(8)),
        (Some(0), Some(3)),
      ),
    )
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val selected = Selection(
      batch,
      0,
      batch.length,
      Seq(
        Selection.Step.Int32(
          0,
          value => {
            keyCalls.incrementAndGet()
            value >= 0
          },
        ),
        Selection.Step.Int32(
          1,
          value => {
            valueCalls.incrementAndGet()
            if (value == 0) then fail("value predicate saw a null payload")
            value > 0
          },
        ),
      ),
    )

    keyCalls.get() shouldBe 3
    valueCalls.get() shouldBe 2
    BatchCodec.decodeNullableIntPairs(selected) shouldBe Seq((Some(4), Some(8)), (Some(0), Some(3)))
  }

  it should "not call a row predicate on a null value that a key filter kept" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq((Some(1), None), (Some(4), Some(8)), (Some(0), Some(3))),
    )
    val rowCalls = new AtomicInteger
    val selected = Selection(
      batch,
      0,
      batch.length,
      Seq(
        Selection.Step.Int32(0, (value: Int) => value >= 0),
        Selection.Step.IntPair((key, value) => {
          rowCalls.incrementAndGet()
          if (value == 0) then fail("row predicate saw a null payload")
          key < 10
        }),
      ),
    )

    rowCalls.get() shouldBe 2
    BatchCodec.decodeNullableIntPairs(selected) shouldBe Seq((Some(4), Some(8)), (Some(0), Some(3)))
  }

  it should "call a general string predicate on an empty cell and leave that cell absent" in {
    val column = DictUtf8Column.fromNullable(Seq(Some("US"), None, Some("CA"), Some("US")))
    val dictionary = column.dictionary
    val firstCalls = new AtomicInteger
    val secondCalls = new AtomicInteger
    val sawAbsent = new AtomicInteger
    val selected = Selection(
      single(column),
      0,
      column.length,
      Seq(
        Selection.Step.Utf8(
          0,
          value => {
            firstCalls.incrementAndGet()
            BatchCodec.isAbsent(value) || isUs(value)
          },
        ),
        Selection.Step.Utf8(
          0,
          value => {
            secondCalls.incrementAndGet()
            if (BatchCodec.isAbsent(value)) then sawAbsent.incrementAndGet()
            BatchCodec.isAbsent(value) || isUs(value)
          },
        ),
      ),
    )
    val out = utf8(selected)

    firstCalls.get() shouldBe 4
    secondCalls.get() shouldBe 3
    sawAbsent.get() shouldBe 1
    BatchCodec.decodeNullableStrings(selected) shouldBe Seq(Some("US"), None, Some("US"))
    out.isValid(0) shouldBe true
    out.isValid(1) shouldBe false
    out.isValid(2) shouldBe true
    out.codes(1) shouldBe 0
    // The filter publishes the partition dictionary. It does not copy it.
    out.dictionary should be theSameInstanceAs dictionary
  }

  it should "name a null triple row and call neither predicate" in {
    val batch = triples(
      Array(1, 2, 3),
      Array(10, 0, 30),
      Array(7, 8, 9),
      Array(true, true, true),
      Array(true, false, true),
      Array(true, true, true),
    )
    val firstCalls = new AtomicInteger
    val secondCalls = new AtomicInteger
    val thrown = the[IllegalArgumentException] thrownBy {
      Selection(
        batch,
        0,
        batch.length,
        Seq(
          Selection.Step.IntTriple(
            0,
            value => {
              firstCalls.incrementAndGet()
              value > 0
            },
          ),
          Selection.Step.IntTriple(
            1,
            _ => {
              secondCalls.incrementAndGet()
              true
            },
          ),
        ),
      )
    }

    thrown.getMessage shouldBe "requirement failed: null Int32 join value at row 1"
    firstCalls.get() shouldBe 0
    secondCalls.get() shouldBe 0
  }

  it should "pre-scan a later triple step before calling it" in {
    val batch = triples(
      Array(0, 2, 3),
      Array(0, 20, 30),
      Array(7, 8, 9),
      Array(true, true, true),
      Array(false, true, true),
      Array(true, true, true),
    )
    val secondCalls = new AtomicInteger
    val thrown = the[IllegalArgumentException] thrownBy {
      Selection(
        batch,
        0,
        batch.length,
        Seq(
          Selection.Step.Int32(0, (value: Int) => value >= 0),
          Selection.Step.IntTriple(
            0,
            _ => {
              secondCalls.incrementAndGet()
              true
            },
          ),
        ),
      )
    }

    thrown.getMessage shouldBe "requirement failed: null Int32 join value at row 0"
    secondCalls.get() shouldBe 0

    val kept = new AtomicInteger
    val selected = Selection(
      batch,
      0,
      batch.length,
      Seq(
        Selection.Step.Int32(0, (value: Int) => value > 0),
        Selection.Step.IntTriple(
          0,
          value => {
            kept.incrementAndGet()
            value < 3
          },
        ),
      ),
    )
    kept.get() shouldBe 2
    BatchCodec.decodeIntTriples(selected) shouldBe Seq((2, (20, 8)))
  }

  it should "match two triple filters on a dense window" in {
    val batch = triples(
      Array(1, 2, 3, 4),
      Array(10, 20, 30, 40),
      Array(7, 8, -1, 9),
      Array(true, true, true, true),
      Array(true, true, true, true),
      Array(true, true, true, true),
    )
    val firstCalls = new AtomicInteger
    val secondCalls = new AtomicInteger
    val selected = Selection(
      batch,
      0,
      batch.length,
      Seq(
        Selection.Step.IntTriple(
          2,
          value => {
            firstCalls.incrementAndGet()
            value > 0
          },
        ),
        Selection.Step.IntTriple(
          0,
          value => {
            secondCalls.incrementAndGet()
            value < 4
          },
        ),
      ),
    )
    val kernel = PairKernel.filterTriple(
      PairKernel.filterTriple(batch, (_, _, right) => right > 0),
      (key, _, _) => key < 4,
    )

    firstCalls.get() shouldBe 4
    secondCalls.get() shouldBe 3
    BatchCodec.decodeIntTriples(selected) shouldBe Seq((1, (10, 7)), (2, (20, 8)))
    BatchCodec.decodeIntTriples(selected) shouldBe BatchCodec.decodeIntTriples(kernel)
  }

  it should "publish an empty batch for an empty range and for a total reject" in {
    val batch = BatchCodec.ints(Seq(1, 2, 3))
    val calls = new AtomicInteger
    ColumnMetrics.withSink(new CountingSink) {
      val empty = Selection(
        batch,
        1,
        0,
        Seq(Selection.Step.Int32(0, _ => { calls.incrementAndGet(); true })),
      )
      empty.length shouldBe 0
      calls.get() shouldBe 0
      ColumnMetrics.batchCount shouldBe 1L
    }
    calls.set(0)
    val rejected = Selection(
      batch,
      0,
      batch.length,
      Seq(Selection.Step.Int32(0, _ => { calls.incrementAndGet(); false })),
    )
    calls.get() shouldBe 3
    rejected.length shouldBe 0
    BatchCodec.decodeInts(rejected) shouldBe Seq.empty[Int]
  }

  private def single(column: Column): ColumnBatch =
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  private def isUs(value: String): Boolean =
    !BatchCodec.isAbsent(value) && value.equals("US")

  private def columnAt(batch: ColumnBatch, ordinal: Int): Column =
    batch.columns.lift(ordinal).getOrElse(fail(s"missing column $ordinal"))

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column =
    columnAt(batch, ordinal) match
      case column: Int64Column => column
      case other => fail(s"expected Int64, got $other")

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
    ),
  )
  private def sameLongPairs(left: Seq[(Long, Long)], right: Seq[(Long, Long)]): Unit =
    left.length shouldBe right.length
    left.zip(right).foreach { (one, other) =>
      component(one, 0) shouldBe component(other, 0)
      component(one, 1) shouldBe component(other, 1)
    }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def component(row: (Long, Long), ordinal: Int): AnyRef =
    row.productElement(ordinal).asInstanceOf[AnyRef]

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    columnAt(batch, ordinal) match
      case column: Int32Column => column
      case other => fail(s"expected Int32, got $other")

  private def float64(batch: ColumnBatch): Float64Column =
    columnAt(batch, 0) match
      case column: Float64Column => column
      case other => fail(s"expected Float64, got $other")

  private def bool(batch: ColumnBatch): BoolColumn =
    columnAt(batch, 0) match
      case column: BoolColumn => column
      case other => fail(s"expected Bool, got $other")

  private def utf8(batch: ColumnBatch): DictUtf8Column =
    columnAt(batch, 0) match
      case column: DictUtf8Column => column
      case other => fail(s"expected Utf8Dict, got $other")

  private def sameInt(left: ColumnBatch, right: ColumnBatch): Unit =
    BatchCodec.decodeInts(left) shouldBe BatchCodec.decodeInts(right)
    val leftColumn = int32(left, 0)
    val rightColumn = int32(right, 0)
    leftColumn.length shouldBe rightColumn.length
    (0 until left.length).foreach { row =>
      leftColumn.isValid(row) shouldBe true
      rightColumn.isValid(row) shouldBe true
    }

  private def triples(
      keys: Array[Int],
      left: Array[Int],
      right: Array[Int],
      keyPresent: Array[Boolean],
      leftPresent: Array[Boolean],
      rightPresent: Array[Boolean],
  ): ColumnBatch =
    val count = keys.length
    def column(values: Array[Int], present: Array[Boolean]): Int32Column =
      Int32Column.of(values, Validity.pack(count, row => present(row)), count)
    val columns = Vector(
      column(keys, keyPresent),
      column(left, leftPresent),
      column(right, rightPresent),
    )
    new ColumnBatch(columns.map(_.logicalType), columns, count)
