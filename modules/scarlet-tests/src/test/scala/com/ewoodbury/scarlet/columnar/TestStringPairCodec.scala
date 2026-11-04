package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Round trip of a UTF-8 dictionary key and an int value. */
class TestStringPairCodec extends AnyFlatSpec with Matchers:

  private val accent = "é"
  private val emoji = "😀"

  "stringIntPairs" should "round-trip null, empty, accented, emoji, and duplicate keys" in {
    val rows = Seq(
      nullKey(1),
      "" -> 2,
      accent -> 3,
      emoji -> 4,
      "red" -> 5,
      "red" -> 6,
    )

    BatchCodec.decodeStringPairs(BatchCodec.stringIntPairs(rows)) shouldBe rows
  }

  it should "decode code 0 as the dictionary string when the bit is set" in {
    val batch = pairBatch(
      DictUtf8Column.of(Array(""), Array(0), Validity.allValid(1), 1),
      Int32Column.of(Array(7), Validity.allValid(1), 1),
      1,
    )

    BatchCodec.decodeStringPairs(batch) shouldBe Seq("" -> 7)
  }

  it should "decode a clear key bit at code 0 as null, not dictionary 0" in {
    val batch = pairBatch(
      DictUtf8Column.of(
        Array("present"),
        Array(0, 0),
        Validity.pack(2, row => row == 1),
        2,
      ),
      Int32Column.of(Array(1, 2), Validity.allValid(2), 2),
      2,
    )

    BatchCodec.decodeStringPairs(batch) shouldBe Seq(nullKey(1), "present" -> 2)
  }

  it should "reject a clear value bit and ignore it past a prefix" in {
    val batch = pairBatch(
      DictUtf8Column.of(Array("red"), Array(0, 0), Validity.allValid(2), 2),
      Int32Column.of(Array(4, 0), Validity.pack(2, row => row == 0), 2),
      2,
    )

    val caught = intercept[IllegalArgumentException] {
      BatchCodec.decodeStringPairs(batch)
    }
    caught.getMessage shouldBe "requirement failed: null Int32 at row 1"
    BatchCodec.decodeStringPairs(batch, 1) shouldBe Seq("red" -> 4)
  }

  it should "reject a batch that is not a string key and an int value" in {
    val caught = intercept[IllegalArgumentException] {
      BatchCodec.decodeStringPairs(BatchCodec.ints(Seq(1, 2)))
    }
    caught.getMessage should include("Utf8Dict")
  }

  private def pairBatch(keys: DictUtf8Column, values: Int32Column, length: Int): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      Vector(keys, values),
      length,
    )

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullKey(value: Int): (String, Int) = (null: String) -> value
