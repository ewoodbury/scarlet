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

  it should "decode a clear value bit as absent and ignore one past a prefix" in {
    val batch = pairBatch(
      DictUtf8Column.of(Array("red"), Array(0, 0), Validity.allValid(2), 2),
      Int32Column.of(Array(4, 0), Validity.pack(2, row => row == 0), 2),
      2,
    )
    val decoded = BatchCodec.decodeStringPairs(batch)

    decoded.length shouldBe 2
    decoded.head shouldBe ("red" -> 4)
    absentValue(decoded(1)) shouldBe true
    keyOf(decoded(1)) shouldBe "red"
    BatchCodec.decodeStringPairs(batch, 1) shouldBe Seq("red" -> 4)
  }

  it should "round-trip a null int as a clear value bit" in {
    val rows = Seq(erased("a", null), erased("", java.lang.Integer.valueOf(0)))
    val batch = BatchCodec.stringIntPairs(rows)
    val values = intColumn(batch)
    val decoded = BatchCodec.decodeStringPairs(batch)

    values.isValid(0) shouldBe false
    values.isValid(1) shouldBe true
    values.values(1) shouldBe 0
    absentValue(decoded.head) shouldBe true
    keyOf(decoded.head) shouldBe "a"
    decoded(1) shouldBe ("" -> 0)
  }

  it should "decode a selected clear value bit and ignore one the index does not name" in {
    val batch = pairBatch(
      DictUtf8Column.of(
        Array("red", ""),
        Array(0, 0, 1),
        Validity.pack(3, row => row != 1),
        3,
      ),
      Int32Column.of(Array(4, 5, 0), Validity.pack(3, row => row != 2), 3),
      3,
    )
    val kept = BatchCodec.decodeStringPairs(batch, Array(0, 1, 99), 2)
    val absent = BatchCodec.decodeStringPairs(batch, Array(0, 2), 2)

    kept shouldBe Seq("red" -> 4, nullKey(5))
    absent.head shouldBe ("red" -> 4)
    keyOf(absent(1)) shouldBe ""
    absentValue(absent(1)) shouldBe true
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

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erased(key: String, value: java.lang.Integer): (String, Int) =
    Tuple2(key, value).asInstanceOf[(String, Int)]

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def absentValue(row: (String, Int)): Boolean =
    row.productElement(1).asInstanceOf[AnyRef] eq null

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def keyOf(row: (String, Int)): String = row.productElement(0) match
    case value: String => value
    case other => fail(s"expected a present string, got $other")

  private def intColumn(batch: ColumnBatch): Int32Column = batch.columns.toList match
    case (_: DictUtf8Column) :: (values: Int32Column) :: Nil => values
    case _ => fail(s"expected a string pair, found ${batch.schema}")
