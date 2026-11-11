package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.execution.JoinDigest

/**
 * Hand-built `(key, left, right)` batches. A clear validity bit is a null value. Decode returns
 * `None` only when the absence bit is set. A null on the optional column is `Some(null)`. A null
 * on the present column stays a bare null.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TestOuterDecode extends AnyFlatSpec with Matchers:

  "optional outer decode" should "read a clear bit with no absence mark as Some(null)" in {
    val right = Int32Column.of(Array(0), Validity.pack(1, _ => false), 1)
    val batch = batchOf(present(Array(1)), present(Array(10)), right)
    right.absence shouldBe None
    right.isValid(0) shouldBe false
    right.isAbsent(0) shouldBe false
    right.values(0) shouldBe 0
    val decoded = BatchCodec.decodeOuter(batch, AbsentSide.Right)
    val row = decoded.headOption.getOrElse(fail("missing row"))
    valueAt(row, 0) shouldBe 10
    assertSomeNull(valueAt(row, 1))
    val digest = JoinDigest.fromOuter(decoded)
    val nullValue = JoinDigest.fromOuter(Seq(bareNullRight(1, 10)))
    val missing = JoinDigest.fromOuter(Seq((1, (10, None: Option[Int]))))
    val zero = JoinDigest.fromOuter(Seq((1, (10, Some(0)))))
    withClue(JoinDigest.difference(digest, nullValue)) {
      JoinDigest.agrees(digest, nullValue) shouldBe true
    }
    JoinDigest.agrees(digest, missing) shouldBe false
    JoinDigest.agrees(digest, zero) shouldBe false
  }

  it should "read an absence bit as None when the payload is zero" in {
    val right = Int32Column.withAbsence(
      Array(0),
      Validity.pack(1, _ => false),
      Validity.pack(1, _ => true),
      1,
    )
    val left = present(Array(10))
    val batch = batchOf(present(Array(1)), left, right)
    right.isAbsent(0) shouldBe true
    right.isValid(0) shouldBe false
    right.values(0) shouldBe 0
    left.isAbsent(0) shouldBe false
    BatchCodec.decodeOuter(batch, AbsentSide.Right) shouldBe Seq((1, (10, None)))
    val thrown = intercept[IllegalArgumentException] {
      right.isAbsent(1)
    }
    thrown.getMessage should include("row 1 outside column length 1")
  }

  it should "read a present zero as Some(0)" in {
    val right = present(Array(0))
    val batch = batchOf(present(Array(1)), present(Array(10)), right)
    right.absence shouldBe None
    right.isValid(0) shouldBe true
    right.isAbsent(0) shouldBe false
    right.values(0) shouldBe 0
    BatchCodec.decodeOuter(batch, AbsentSide.Right) shouldBe Seq((1, (10, Some(0))))
  }

  it should "keep a clear bit on the present side as null" in {
    val left = Int32Column.of(Array(0), Validity.pack(1, _ => false), 1)
    val batch = batchOf(present(Array(1)), left, present(Array(20)))
    left.absence shouldBe None
    left.isValid(0) shouldBe false
    left.isAbsent(0) shouldBe false
    val decoded = BatchCodec.decodeOuter(batch, AbsentSide.Right)
    val row = decoded.headOption.getOrElse(fail("missing row"))
    assertBareNull(valueAt(row, 0))
    valueAt(row, 1) shouldBe Some(20)
    val digest = JoinDigest.fromOuter(decoded)
    val nullValue = JoinDigest.fromOuter(Seq(bareNullLeft(1, 20)))
    val missing = JoinDigest.fromOuter(Seq((1, (None: Option[Int], 20))))
    withClue(JoinDigest.difference(digest, nullValue)) {
      JoinDigest.agrees(digest, nullValue) shouldBe true
    }
    JoinDigest.agrees(digest, missing) shouldBe false
  }

  it should "decode an empty batch as no rows" in {
    val column = Int32Column.of(Array.empty[Int], Validity.allValid(0), 0)
    val batch = batchOf(column, column, column)
    batch.length shouldBe 0
    BatchCodec.decodeOuter(batch, AbsentSide.Right) shouldBe Seq.empty
    BatchCodec.decodeOuter(batch, AbsentSide.Left) shouldBe Seq.empty
    BatchCodec.decodeOuter(batch, AbsentSide.Both) shouldBe Seq.empty
  }

  it should "decode a batch where every row is absent as None" in {
    val right = Int32Column.withAbsence(
      Array(0, 0),
      Validity.pack(2, _ => false),
      Validity.pack(2, _ => true),
      2,
    )
    val batch = batchOf(present(Array(1, 2)), present(Array(10, 20)), right)
    right.isAbsent(0) shouldBe true
    right.isAbsent(1) shouldBe true
    BatchCodec.decodeOuter(batch, AbsentSide.Right) shouldBe Seq(
      (1, (10, None)),
      (2, (20, None)),
    )
  }

  it should "read a clear bit beside a set absence bit as Some(null)" in {
    val right = Int32Column.withAbsence(
      Array(0, 0, 0),
      Validity.pack(3, row => row == 2),
      Validity.pack(3, row => row == 1),
      3,
    )
    val batch = batchOf(present(Array(1, 2, 3)), present(Array(10, 20, 30)), right)
    right.absence.isDefined shouldBe true
    right.isValid(0) shouldBe false
    right.isAbsent(0) shouldBe false
    right.isAbsent(1) shouldBe true
    right.isValid(2) shouldBe true
    right.values(2) shouldBe 0
    val decoded = BatchCodec.decodeOuter(batch, AbsentSide.Right)
    val cells = decoded.map(row => valueAt(row, 1))
    cells.length shouldBe 3
    assertSomeNull(cells.headOption.getOrElse(fail("missing null row")))
    cells.lift(1).getOrElse(fail("missing absent row")) shouldBe None
    cells.lift(2).getOrElse(fail("missing zero row")) shouldBe Some(0)
  }

  private def batchOf(key: Int32Column, left: Int32Column, right: Int32Column): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
      Vector(key, left, right),
      key.length,
    )

  private def present(values: Array[Int]): Int32Column =
    Int32Column.of(values, Validity.allValid(values.length), values.length)

  /** `productElement` keeps a null inside the value pair. Tuple `==` unboxes it. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
    ),
  )
  private def valueAt(row: Any, ordinal: Int): Any =
    val values = row.asInstanceOf[Product].productElement(1).asInstanceOf[Product]
    values.productElement(ordinal)

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def assertSomeNull(cell: Any): org.scalatest.Assertion = cell match
    case Some(null) => succeed
    case other => fail(s"expected Some(null), got $other")

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def assertBareNull(cell: Any): org.scalatest.Assertion = cell match
    case null => succeed
    case Some(_) => fail(s"present side was wrapped: $cell")
    case other => fail(s"expected a null int, got $other")

  /**
   * A right value that is null, not `None`. `Tuple2` keeps the null. A pair literal would unbox
   * it to `0`. [[JoinDigest.fromOuter]] classifies that null as a null value.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def bareNullRight(key: Int, left: Int): (Int, (Int, Int)) =
    Tuple2(Int.box(key), Tuple2(Int.box(left), null)).asInstanceOf[(Int, (Int, Int))]

  /** A left value that is null, not `None`. Same erasure boundary as [[bareNullRight]]. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def bareNullLeft(key: Int, right: Int): (Int, (Int, Int)) =
    Tuple2(Int.box(key), Tuple2(null, Int.box(right))).asInstanceOf[(Int, (Int, Int))]

object TestOuterDecode:

  /**
   * `(key, left, right)`. Keys are present. A side whose flags are all true stores no absence
   * bitmap. A clear flag sets that row's absence bit and leaves the validity bit clear.
   */
  def outerBatch(
      keys: Array[Int],
      left: Array[Int],
      right: Array[Int],
      leftPresent: Array[Boolean],
      rightPresent: Array[Boolean],
  ): ColumnBatch =
    val width = keys.length
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keys, Validity.allValid(width), width),
        valueColumn(left, leftPresent),
        valueColumn(right, rightPresent),
      ),
      width,
    )

  private def valueColumn(values: Array[Int], presentFlags: Array[Boolean]): Int32Column =
    val width = values.length
    val validity = Validity.pack(width, row => presentFlags(row))
    if (presentFlags.forall(bit => bit)) then Int32Column.of(values, validity, width)
    else
      Int32Column.withAbsence(
        values,
        validity,
        Validity.pack(width, row => !presentFlags(row)),
        width,
      )
