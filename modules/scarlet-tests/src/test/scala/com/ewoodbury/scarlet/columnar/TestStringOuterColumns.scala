package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Hand-built string outer batches. A clear key bit is null and is not `dictionary(0)`. An absence
 * bit is a missing partner. An int triple still decodes. A long triple is refused.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
class TestStringOuterColumns extends AnyFlatSpec with Matchers:

  "string outer decode" should "read an absence bit as None when the payload is zero" in {
    val right = Int32Column.withAbsence(
      Array(0),
      Validity.pack(1, _ => false),
      Validity.pack(1, _ => true),
      1,
    )
    val batch = triple(presentKey(Array("a")), present(Array(10)), right)
    right.isAbsent(0) shouldBe true
    right.isValid(0) shouldBe false
    right.values(0) shouldBe 0
    StringOuterDecode.decode(batch, AbsentSide.Right) shouldBe Seq(("a", (10, None)))
    val thrown = intercept[IllegalArgumentException] {
      val _ = right.isAbsent(1)
    }
    thrown.getMessage should include("outside column length")
  }

  it should "reject a clear bit that is not an absence mark" in {
    val right = Int32Column.of(Array(0), Validity.pack(1, _ => false), 1)
    val batch = triple(presentKey(Array("a")), present(Array(10)), right)
    right.absence shouldBe None
    val thrown = intercept[IllegalArgumentException] {
      val _ = StringOuterDecode.decode(batch, AbsentSide.Right)
    }
    thrown.getMessage should include("null Int32 at row 0 on an optional outer column")
  }

  it should "keep a clear key bit as null when code 0 is a real string" in {
    val keys = DictUtf8Column.of(
      Array("a"),
      Array(0),
      Validity.pack(1, _ => false),
      1,
    )
    val batch = triple(keys, present(Array(10)), present(Array(20)))
    val decoded = StringOuterDecode.decode(batch, AbsentSide.Right)
    decoded shouldBe Seq((null, (10, Some(20))))
  }

  it should "delegate an int triple and refuse a long triple" in {
    val ints = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
      Vector(present(Array(1)), present(Array(10)), present(Array(20))),
      1,
    )
    StringOuterDecode.decode(ints, AbsentSide.Right) shouldBe BatchCodec.decodeOuter(
      ints,
      AbsentSide.Right,
    )
    val longs = new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int64Column.of(Array(1L), Validity.allValid(1), 1),
        Int64Column.of(Array(10L), Validity.allValid(1), 1),
        Int64Column.of(Array(20L), Validity.allValid(1), 1),
      ),
      1,
    )
    val thrown = intercept[IllegalArgumentException] {
      val _ = StringOuterDecode.decode(longs, AbsentSide.Right)
    }
    thrown.getMessage should include(longs.schema.toString)
    (thrown.getMessage == "outer join output is not three Int32 columns") shouldBe false
  }

  "string outer concat" should "keep a missing partner and a null key" in {
    val absentRight = Int32Column.withAbsence(
      Array(0),
      Validity.pack(1, _ => false),
      Validity.pack(1, _ => true),
      1,
    )
    val first = triple(presentKey(Array("b")), present(Array(1)), absentRight)
    val nullKey = DictUtf8Column.of(
      Array("a"),
      Array(0),
      Validity.pack(1, _ => false),
      1,
    )
    val second = triple(nullKey, present(Array(2)), present(Array(3)))
    val joined = ColumnBatches.concat(Vector(first, second))
    StringOuterDecode.decode(joined, AbsentSide.Right) should contain theSameElementsAs Seq(
      ("b", (1, None)),
      (null, (2, Some(3))),
    )
  }

  private def triple(key: DictUtf8Column, left: Int32Column, right: Int32Column): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32, LogicalType.Int32),
      Vector(key, left, right),
      key.length,
    )

  private def presentKey(words: Array[String]): DictUtf8Column =
    DictUtf8Column.of(
      words,
      Array.tabulate(words.length)(identity),
      Validity.allValid(words.length),
      words.length,
    )

  private def present(values: Array[Int]): Int32Column =
    Int32Column.of(values, Validity.allValid(values.length), values.length)
