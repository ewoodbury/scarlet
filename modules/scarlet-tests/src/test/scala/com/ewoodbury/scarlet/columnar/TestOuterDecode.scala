package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Hand-built `(key, left, right)` batches. A clear validity bit is a null value. Decode returns
 * `None` only when the absence bit is set.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TestOuterDecode extends AnyFlatSpec with Matchers:

  "optional outer decode" should "reject a clear bit that is not an absence mark" in {
    val right = Int32Column.of(Array(0), Validity.pack(1, _ => false), 1)
    val batch = batchOf(present(Array(1)), present(Array(10)), right)
    right.absence shouldBe None
    right.isValid(0) shouldBe false
    right.isAbsent(0) shouldBe false
    val thrown = intercept[IllegalArgumentException] {
      BatchCodec.decodeOuter(batch, AbsentSide.Right)
    }
    thrown.getMessage should include("null Int32 at row 0 on an optional outer column")
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

  private def batchOf(key: Int32Column, left: Int32Column, right: Int32Column): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
      Vector(key, left, right),
      key.length,
    )

  private def present(values: Array[Int]): Int32Column =
    Int32Column.of(values, Validity.allValid(values.length), values.length)

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
