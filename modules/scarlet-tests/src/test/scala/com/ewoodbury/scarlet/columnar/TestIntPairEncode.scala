package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Boxed int-pair encode. A null is a hole. [[BatchCodec.intPairs]] still unboxes. */
class TestIntPairEncode extends AnyFlatSpec with Matchers:

  "pairs" should "stay all-valid and skip primitiveEncode when every cell is present" in {
    val rows = Seq((1, 10), (2, 0), (-4, 7))
    val (batch, batches, encodes) = measured(BatchCodec.intPairsPreservingAbsence(rows))

    batches shouldBe 1L
    encodes shouldBe 0L
    BatchCodec.decodeIntPairs(batch) shouldBe rows
    fullyPresent(batch) shouldBe true
  }

  it should "record holes across a word boundary and store zero in a null slot" in {
    val rows = Array.tabulate(65)(row => (row, row + 1))
    rows(1) = erased(Some(1), None)
    rows(63) = erased(None, Some(70))
    rows(64) = erased(Some(64), None)
    val batch = BatchCodec.intPairsPreservingAbsence(rows.toSeq)
    val keys = int32(batch, 0)
    val values = int32(batch, 1)

    batch.length shouldBe 65
    keys.isValid(0) shouldBe true
    values.isValid(0) shouldBe true
    keys.isValid(1) shouldBe true
    keys.values(1) shouldBe 1
    values.isValid(1) shouldBe false
    values.values(1) shouldBe 0
    keys.isValid(63) shouldBe false
    keys.values(63) shouldBe 0
    values.isValid(63) shouldBe true
    values.values(63) shouldBe 70
    keys.isValid(64) shouldBe true
    values.isValid(64) shouldBe false
    BatchCodec.decodeNullableIntPairs(batch).lift(64) shouldBe Some((Some(64), None))
  }

  it should "grow the hole list past the first eight nulls" in {
    val rows = Array.tabulate(10)(row => (row, row))
    (1 to 9).foreach(row => rows(row) = erased(Some(row), None))
    val batch = BatchCodec.intPairsPreservingAbsence(rows.toSeq)
    val keys = int32(batch, 0)
    val values = int32(batch, 1)

    keys.isValid(0) shouldBe true
    values.isValid(0) shouldBe true
    values.values(0) shouldBe 0
    (1 to 9).foreach { row =>
      keys.isValid(row) shouldBe true
      keys.values(row) shouldBe row
      values.isValid(row) shouldBe false
      values.values(row) shouldBe 0
    }
  }

  it should "encode an empty iterable as an empty all-valid batch" in {
    val batch = BatchCodec.intPairsPreservingAbsence(Seq.empty[(Int, Int)])

    batch.length shouldBe 0
    fullyPresent(batch) shouldBe true
    BatchCodec.decodeNullableIntPairs(batch) shouldBe Seq.empty[(Option[Int], Option[Int])]
  }

  it should "reject a non-int cell with the pair-component message" in {
    val thrown = the[IllegalArgumentException] thrownBy BatchCodec.intPairsPreservingAbsence(
      Seq((1, 2), nonIntValue),
    )
    thrown.getMessage shouldBe "pair component expected an Int value, found java.lang.String"
  }

  "side" should "read one ordinal and keep a null there" in {
    val rows = Seq(erased(Some(1), Some(10)), erased(None, Some(4)), erased(Some(3), None))
    val keys = IntPairEncode.side(rows, 0)
    val values = IntPairEncode.side(rows, 1)
    val keyColumn = int32(keys, 0)
    val valueColumn = int32(values, 0)

    keys.length shouldBe 3
    keyColumn.isValid(1) shouldBe false
    keyColumn.values(1) shouldBe 0
    keyColumn.isValid(2) shouldBe true
    keyColumn.values(2) shouldBe 3
    valueColumn.isValid(1) shouldBe true
    valueColumn.values(1) shouldBe 4
    valueColumn.isValid(2) shouldBe false
  }

  it should "ignore a non-int on the ordinal it does not read" in {
    val rows = Seq(Tuple2("no", Int.box(4)): Product)
    val values = IntPairEncode.side(rows, 1)
    val column = int32(values, 0)

    values.schema shouldBe Vector(LogicalType.Int32)
    column.isValid(0) shouldBe true
    column.values(0) shouldBe 4

    val thrown = the[IllegalArgumentException] thrownBy IntPairEncode.side(rows, 0)
    thrown.getMessage shouldBe "pair component expected an Int key, found java.lang.String"
  }

  "intPairs" should "still unbox a null to zero" in {
    val batch = BatchCodec.intPairs(Seq((1, 2), erased(Some(3), None)))
    val values = int32(batch, 1)

    fullyPresent(batch) shouldBe true
    values.values(1) shouldBe 0
    BatchCodec.decodeIntPairs(batch) shouldBe Seq((1, 2), (3, 0))
  }

  "except" should "clear the named holes and stay all-valid when the hole count is zero" in {
    val cleared = Validity.except(70, Array(0, 63, 64, 63), 4)
    val untouched = Validity.except(8, Array.empty[Int], 0)

    cleared.isValid(0) shouldBe false
    cleared.isValid(1) shouldBe true
    cleared.isValid(63) shouldBe false
    cleared.isValid(64) shouldBe false
    cleared.isValid(65) shouldBe true
    (0 until 8).foreach(row => untouched.isValid(row) shouldBe true)
    val separate = Validity.allValid(70)
    Validity.except(70, Array(0), 1)
    separate.isValid(0) shouldBe true
  }

  private def measured(body: => ColumnBatch): (ColumnBatch, Long, Long) =
    ColumnMetrics.withSink(new CountingSink) {
      val batch = body
      (batch, ColumnMetrics.batchCount, ColumnMetrics.primitiveEncodeCount)
    }

  private def fullyPresent(batch: ColumnBatch): Boolean =
    batch.columns.forall(column => (0 until column.length).forall(column.isValid))

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  /** Named erasure boundary. The value cell is a `String`, not an `Int`. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def nonIntValue: (Int, Int) =
    Tuple2(Int.box(1), "no").asInstanceOf[(Int, Int)]

  /** Named erasure boundary. `Tuple2` keeps a null `Integer`; `(Int, Int)` would unbox it. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erased(key: Option[Int], value: Option[Int]): (Int, Int) =
    val left: AnyRef = key match
      case Some(present) => Int.box(present)
      case None => null
    val right: AnyRef = value match
      case Some(present) => Int.box(present)
      case None => null
    Tuple2(left, right).asInstanceOf[(Int, Int)]
