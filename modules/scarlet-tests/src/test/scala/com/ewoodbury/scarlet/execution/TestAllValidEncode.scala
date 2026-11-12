package com.ewoodbury.scarlet.execution

import scala.collection.immutable.ArraySeq

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{BoolArrays, Partition, StringArrays}

/** Owned string and boolean encode against the iterable encoders. */
class TestAllValidEncode extends AnyFlatSpec with Matchers:

  "all-valid strings" should "match the iterable encoder on one owned array" in {
    val values = Array("b", "a", "b", "", "a", "c")
    sameStrings(values)
    val batch = BatchCodec.allValidStrings(values)
    val column = utf8(batch)
    column.dictionary.toSeq shouldBe Seq("b", "a", "", "c")
    column.codes.toSeq shouldBe Seq(0, 1, 0, 2, 1, 3)
    values(0) = "later"
    column.dictionary.toSeq shouldBe Seq("b", "a", "", "c")
    BatchCodec.decodeStrings(batch) shouldBe Seq("b", "a", "b", "", "a", "c")
  }

  it should "match on an empty array" in {
    sameStrings(Array.empty[String])
  }

  it should "match on one element" in {
    val values = Array("only")
    sameStrings(values)
    val column = utf8(BatchCodec.allValidStrings(values))
    column.dictionary.toSeq shouldBe Seq("only")
    column.codes.toSeq shouldBe Seq(0)
  }

  it should "share one code for duplicate strings" in {
    val values = Array("red", "red", "red")
    sameStrings(values)
    val column = utf8(BatchCodec.allValidStrings(values))
    column.dictionary.toSeq shouldBe Seq("red")
    column.codes.toSeq shouldBe Seq(0, 0, 0)
  }

  it should "keep an empty string present" in {
    val values = Array("", "")
    sameStrings(values)
    val column = utf8(BatchCodec.allValidStrings(values))
    column.dictionary.toSeq shouldBe Seq("")
    column.codes.toSeq shouldBe Seq(0, 0)
    column.isValid(0) shouldBe true
    column.isValid(1) shouldBe true
    BatchCodec.decodeStrings(BatchCodec.allValidStrings(values)) shouldBe Seq("", "")
  }

  it should "keep a null absent on the iterable path" in {
    val absent = BatchCodec.stringElement(None)
    val iterable = DictUtf8Column.fromIterable(Seq("a", absent, ""))
    val nullable = DictUtf8Column.fromNullable(Seq(Some("a"), None, Some("")))
    val boxed = utf8(only(SourceEncode.encodeStrings(Seq(Partition(Seq("a", absent, ""))))))
    sameAbsent(iterable)
    sameAbsent(nullable)
    sameAbsent(boxed)
    val decoded = only(SourceEncode.encodeStrings(Seq(Partition(Seq("a", absent, "")))))
    BatchCodec.decodeStrings(decoded) shouldBe Seq("a", absent, "")
  }

  "all-valid bools" should "write the same bytes for a true and false mix" in {
    sameBools(Array(true, false, true, false, false))
    sameBools(Array.tabulate(70)(index => (index % 3) != 0))
  }

  it should "write the same bytes for one true, one false, and an empty array" in {
    sameBools(Array(true))
    sameBools(Array(false))
    sameBools(Array.empty[Boolean])
  }

  private def sameStrings(values: Array[String]): Unit =
    val owned = DictUtf8Column.allValid(values)
    val iterable = DictUtf8Column.fromIterable(ArraySeq.unsafeWrapArray(values))
    owned.codes.toSeq shouldBe iterable.codes.toSeq
    owned.dictionary.toSeq shouldBe iterable.dictionary.toSeq
    owned.validity.words.toSeq shouldBe iterable.validity.words.toSeq
    owned.validity.words.toSeq shouldBe Validity.allValid(values.length).words.toSeq
    val batch = BatchCodec.allValidStrings(values)
    utf8(batch).codes.toSeq shouldBe owned.codes.toSeq
    utf8(batch).dictionary.toSeq shouldBe owned.dictionary.toSeq
    BatchCodec.decodeStrings(batch) shouldBe values.toSeq

    val fromOwned = utf8(only(SourceEncode.encodeStrings(Seq(Partition(StringArrays(values))))))
    val fromBoxed = utf8(only(SourceEncode.encodeStrings(Seq(Partition(values.toVector)))))
    fromOwned.codes.toSeq shouldBe owned.codes.toSeq
    fromOwned.dictionary.toSeq shouldBe owned.dictionary.toSeq
    fromOwned.validity.words.toSeq shouldBe owned.validity.words.toSeq
    fromBoxed.codes.toSeq shouldBe owned.codes.toSeq
    fromBoxed.dictionary.toSeq shouldBe owned.dictionary.toSeq
    fromBoxed.validity.words.toSeq shouldBe owned.validity.words.toSeq
    val decoded = only(SourceEncode.encodeStrings(Seq(Partition(StringArrays(values)))))
    BatchCodec.decodeStrings(decoded) shouldBe values.toSeq

  private def sameAbsent(column: DictUtf8Column): Unit =
    column.isValid(0) shouldBe true
    column.isValid(1) shouldBe false
    column.isValid(2) shouldBe true
    column.dictionary.toSeq shouldBe Seq("a", "")
    column.codes.toSeq shouldBe Seq(0, 0, 1)

  private def sameBools(values: Array[Boolean]): Unit =
    val expected = values.map(bit => if (bit) 1.toByte else 0.toByte).toSeq
    val owned = boolColumn(BatchCodec.copiedBools(values))
    val boxed = boolColumn(BatchCodec.bools(values.toVector))
    val wrapped = boolColumn(BatchCodec.bools(new ArraySeq.ofBoolean(values)))
    owned.values.toSeq shouldBe expected
    boxed.values.toSeq shouldBe expected
    wrapped.values.toSeq shouldBe expected
    owned.validity.words.toSeq shouldBe Validity.allValid(values.length).words.toSeq
    boxed.validity.words.toSeq shouldBe owned.validity.words.toSeq
    wrapped.validity.words.toSeq shouldBe owned.validity.words.toSeq
    BatchCodec.decodeBools(BatchCodec.copiedBools(values)) shouldBe values.toSeq
    val encoded = only(SourceEncode.encodeBools(Seq(Partition(BoolArrays(values)))))
    BatchCodec.decodeBools(encoded) shouldBe values.toSeq
    boolColumn(encoded).values.toSeq shouldBe expected
    boolColumn(encoded).validity.words.toSeq shouldBe owned.validity.words.toSeq

  private def only(batches: Vector[ColumnBatch]): ColumnBatch =
    batches match
      case Vector(batch) => batch
      case other => fail(s"expected one batch, got ${other.length}")

  private def utf8(batch: ColumnBatch): DictUtf8Column =
    batch.columns match
      case Seq(column: DictUtf8Column) => column
      case other => fail(s"expected a DictUtf8 column, got $other")

  private def boolColumn(batch: ColumnBatch): BoolColumn =
    batch.columns match
      case Seq(column: BoolColumn) => column
      case other => fail(s"expected a Bool column, got $other")
