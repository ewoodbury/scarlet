package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * `(String, Int) => (String, Int)` over one batch.
 *
 * The function runs on every row, including a null key. The key column shares the input
 * dictionary. A key that dictionary does not hold declines before the batch is published. This
 * kernel does not intern.
 */
class TestStringPairProject extends AnyFlatSpec with Matchers:

  private val dictionaryMiss =
    "string pair map cannot share the input dictionary: the key is not in it"

  "StringPairProject" should "publish an empty slice without calling the function" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 1, "b" -> 2))
    val calls = new AtomicInteger
    val out = StringPairProject.project(batch, 2, 0, counting(calls, keep))
    out.length shouldBe 0
    calls.get() shouldBe 0
    utf8(out).dictionary should be theSameInstanceAs utf8(batch).dictionary
    val key = utf8(out)
    val value = intColumn(out, 1)
    key.validity.words should not be theSameInstanceAs(value.validity.words)
  }

  it should "map the slice that starts after the first row" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4))
    val calls = new AtomicInteger
    val out = StringPairProject.project(batch, 1, 2, counting(calls, shiftValue))
    calls.get() shouldBe 2
    cells(out) shouldBe Seq("b:3", "c:4")
    utf8(out).dictionary should be theSameInstanceAs utf8(batch).dictionary
    utf8(out).codes.slice(0, 2).toSeq shouldBe utf8(batch).codes.slice(1, 3).toSeq
  }

  it should "keep the input code when the key text is equal but not the same instance" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 1, "b" -> 2))
    val out = StringPairProject.project(batch, 0, 2, copyKey)
    utf8(out).dictionary should be theSameInstanceAs utf8(batch).dictionary
    utf8(out).codes.toSeq shouldBe utf8(batch).codes.toSeq
    cells(out) shouldBe Seq("a:2", "b:3")
  }

  it should "reuse a code already in the dictionary when the key changes" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 1, "b" -> 2, "a" -> 3))
    val out = StringPairProject.project(batch, 0, 3, swapKnown)
    utf8(out).dictionary should be theSameInstanceAs utf8(batch).dictionary
    utf8(out).codes.toSeq shouldBe Seq(1, 0, 1)
    cells(out) shouldBe Seq("b:1", "a:2", "b:3")
  }

  it should "decline a key the dictionary does not hold" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 1, "b" -> 2))
    val thrown = intercept[ColumnarFallback] {
      StringPairProject.project(batch, 0, 2, invent)
    }
    thrown.getMessage shouldBe dictionaryMiss
  }

  it should "decline a null, a non-pair, and a non-string key" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 1))
    val messages = Seq(constant(nullResult), constant("no"), constant((1, 2))).map { mapper =>
      val thrown = intercept[ColumnarFallback] {
        StringPairProject.project(batch, 0, 1, mapper)
      }
      thrown.getMessage
    }
    messages.foreach(_.startsWith("pair map did not stay (String, Int)") shouldBe true)
  }

  it should "call the function on a null key, a null value, and both" in {
    val batch = BatchCodec.stringIntPairs(
      Seq(
        erased("a", Int.box(1)),
        erased(null, Int.box(2)),
        erased("b", null),
        erased(null, null),
      ),
    )
    val calls = new AtomicInteger
    val out = StringPairProject.project(batch, 0, 4, adaptCounted(calls, rewriteNull))
    calls.get() shouldBe 4
    cells(out) shouldBe Seq("a:1", "a:2", "b:null", "a:null")
    utf8(out).dictionary should be theSameInstanceAs utf8(batch).dictionary
  }

  it should "give a null result key its own validity words" in {
    val length = 3
    val held = Validity.allValid(length)
    val before = held.words.clone()
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 1, "b" -> 2, "c" -> 3))
    val out = StringPairProject.project(batch, 0, length, dropKey("b"))
    val key = utf8(out)
    val value = intColumn(out, 1)
    key.isValid(0) shouldBe true
    key.isValid(1) shouldBe false
    key.isValid(2) shouldBe true
    value.isValid(0) shouldBe true
    value.isValid(1) shouldBe true
    value.isValid(2) shouldBe true
    key.validity.words should not be theSameInstanceAs(value.validity.words)
    key.validity.words should not be theSameInstanceAs(held.words)
    value.validity.words should not be theSameInstanceAs(held.words)
    key.dictionary should be theSameInstanceAs utf8(batch).dictionary
    held.words.toSeq shouldBe before.toSeq
    cells(out) shouldBe Seq("a:1", "null:2", "c:3")
  }

  private def keep(key: String, value: Int): (String, Int) = (key, value)

  private def shiftValue(key: String, value: Int): (String, Int) = (key, value + 1)

  /** A copied key. Equality is by text, so the input code is reused and the map is not built. */
  private def copyKey: StringPairProject.StringPairMap =
    StringPairProject.adapt((pair: (String, Int)) => (new String(pair._1), pair._2 + 1))

  /** Swap two keys that are already in the dictionary. */
  private def swapKnown: StringPairProject.StringPairMap =
    StringPairProject.adapt((pair: (String, Int)) => {
      val next = if (pair._1 == "a") "b" else "a"
      (next, pair._2)
    })

  private def invent: StringPairProject.StringPairMap =
    StringPairProject.adapt((pair: (String, Int)) => (pair._1 + "!", pair._2))

  /** A null key becomes "a", which the fixture dictionary already holds. A null value stays null. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Throw",
    ),
  )
  private def rewriteNull(pair: (String, Int)): (String, Int) =
    val product = pair.asInstanceOf[Product]
    val value = product.productElement(1)
    product.productElement(0) match
      case null => erased("a", value)
      case present: String => erased(present, value)
      case other => throw new IllegalArgumentException(s"pair map saw $other")

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
    ),
  )
  private def dropKey(dropped: String): StringPairProject.StringPairMap =
    new StringPairProject.StringPairMap:
      def apply(key: String, value: Int): Any =
        if (key == dropped) then erased(null, Int.box(value)) else (key, value)
      def applyNullValue(key: String): Any = apply(key, 0)
      def applyNullKey(value: Int): Any = apply("", value)
      def applyNulls(): Any = erased(null, null)

  private def adaptCounted(
      calls: AtomicInteger,
      fn: ((String, Int)) => (String, Int),
  ): StringPairProject.StringPairMap =
    StringPairProject.adapt((pair: (String, Int)) => {
      calls.incrementAndGet()
      fn(pair)
    })

  private def counting(
      calls: AtomicInteger,
      fn: (String, Int) => (String, Int),
  ): StringPairProject.StringPairMap =
    StringPairProject.adapt((pair: (String, Int)) => {
      calls.incrementAndGet()
      fn(pair._1, pair._2)
    })

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullResult: Any = null

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def constant(result: Any): StringPairProject.StringPairMap =
    new StringPairProject.StringPairMap:
      def apply(key: String, value: Int): Any = result
      def applyNullValue(key: String): Any = result
      def applyNullKey(value: Int): Any = result
      def applyNulls(): Any = result

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erased(key: Any, value: Any): (String, Int) =
    Tuple2(key, value).asInstanceOf[(String, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.SeqApply"))
  private def utf8(batch: ColumnBatch): DictUtf8Column =
    batch.columns(0) match
      case column: DictUtf8Column => column
      case other => fail(s"column 0 was ${other.getClass.getName}")

  @SuppressWarnings(Array("org.wartremover.warts.SeqApply"))
  private def intColumn(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns(ordinal) match
      case column: Int32Column => column
      case other => fail(s"column $ordinal was ${other.getClass.getName}")

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def cells(batch: ColumnBatch): Seq[String] =
    BatchCodec.decodeStringPairs(batch).map { row =>
      val product = row.asInstanceOf[Product]
      String.valueOf(product.productElement(0)) + ":" + String.valueOf(product.productElement(1))
    }
