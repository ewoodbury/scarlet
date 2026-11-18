package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * `(String, Int) => (String, Int)` over one batch.
 *
 * The function runs on every row in the slice, including a null key. An empty slice does not call
 * it. `""` is a present key. A result that is not a pair of a string and an int declines before
 * the batch is published. Each output batch owns its dictionary. A null cell owns its validity
 * words, and [[Validity.allValid]] is not the array those words come from.
 */
class TestStringPairProject extends AnyFlatSpec with Matchers:

  "StringPairProject" should "publish an empty slice without calling the function" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 1, "b" -> 2))
    val calls = new AtomicInteger
    StringPairProject.resetPublishedForTest()
    val out = StringPairProject.project(batch, 2, 0, counting(calls, keep))
    out.length shouldBe 0
    calls.get() shouldBe 0
    StringPairProject.publishedForTest shouldBe 1
    val key = utf8(out)
    val value = intColumn(out, 1)
    key.validity.words should not be theSameInstanceAs(value.validity.words)
  }

  it should "map the slice that starts after the first row" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4))
    val calls = new AtomicInteger
    val out = StringPairProject.project(batch, 1, 2, counting(calls, shift))
    calls.get() shouldBe 2
    out.length shouldBe 2
    cells(out) shouldBe Seq("b!:3", "c!:4")
  }

  it should "decline a null, a non-pair, and a cell that is not a string or an int" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 1))
    StringPairProject.resetPublishedForTest()
    val messages = Seq(constant(nullResult), constant("no"), constant((1, 2)), constant(("a", 1L)))
      .map { mapper =>
        val thrown = intercept[ColumnarFallback] {
          StringPairProject.project(batch, 0, 1, mapper)
        }
        thrown.getMessage
      }
    messages.foreach(_ should startWith("pair map did not stay (String, Int)"))
    StringPairProject.publishedForTest shouldBe 0
  }

  it should "call the function on a null key, a null value, and both" in {
    val batch = nullablePairs(
      Seq(
        (Some("a"), Some(1)),
        (None, Some(9)),
        (Some(""), None),
        (None, None),
      ),
    )
    val calls = new AtomicInteger
    val out = StringPairProject.project(batch, 0, 4, adaptCounted(calls, rewriteNull))
    calls.get() shouldBe 4
    cells(out) shouldBe Seq("a!:1", "n:9", "!:null", "n:null")
  }

  it should "keep an empty string present, including when its code is zero" in {
    val batch = BatchCodec.stringIntPairs(Seq("" -> 4, "a" -> 5))
    val out = StringPairProject.project(batch, 0, 2, counting(new AtomicInteger, keep))
    val keys = utf8(out)
    keys.isValid(0) shouldBe true
    keys.isValid(1) shouldBe true
    keys.codes(0) shouldBe 0
    keys.dictionary(0) shouldBe ""
    cells(out) shouldBe Seq(":4", "a:5")
  }

  it should "keep the two accented forms as different dictionary entries" in {
    val composed = "é"
    val decomposed = "e\u0301"
    val batch = BatchCodec.stringIntPairs(Seq(composed -> 1, decomposed -> 2, composed -> 3))
    val out = StringPairProject.project(batch, 0, 3, counting(new AtomicInteger, keep))
    val dictionary = utf8(out).dictionary
    dictionary.toSeq shouldBe Seq(composed, decomposed)
    dictionary(0) should not be dictionary(1)
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
    held.words.toSeq shouldBe before.toSeq
    cells(out) shouldBe Seq("a:1", "null:2", "c:3")
  }

  it should "give each output batch its own dictionary" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 1, "b" -> 2))
    val input = utf8(batch).dictionary
    val snapshot = input.toSeq
    val first = StringPairProject.project(batch, 0, 2, counting(new AtomicInteger, shift))
    val second = StringPairProject.project(batch, 0, 2, counting(new AtomicInteger, shift))
    val left = utf8(first).dictionary
    val right = utf8(second).dictionary
    left should not be theSameInstanceAs(right)
    left should not be theSameInstanceAs(input)
    right should not be theSameInstanceAs(input)
    input.toSeq shouldBe snapshot
    left.toSeq shouldBe Seq("a!", "b!")
    right.toSeq shouldBe Seq("a!", "b!")
  }

  private def keep(key: String, value: Int): (String, Int) = (key, value)

  private def shift(key: String, value: Int): (String, Int) = (key + "!", value + 1)

  /** A null key becomes `n`. A present key gains `!`. A null value stays null. */
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
      case null => erased("n", value)
      case present: String => erased(present + "!", value)
      case other => throw new IllegalArgumentException(s"pair map saw $other")

  /** Dense input. Key `dropped` becomes null. The value stays. */
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
        if (key == dropped) then erased(null, Integer.valueOf(value)) else (key, value)
      def applyNullValue(key: String): Any = erased(key, null)
      def applyNullKey(value: Int): Any = erased(null, Integer.valueOf(value))
      def applyNulls(): Any = erased(null, null)

  /** Passes the pair through. `pair._1` would unbox a null key before [[rewriteNull]] sees it. */
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

  private def nullablePairs(rows: Seq[(Option[String], Option[Int])]): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      Vector(
        DictUtf8Column.fromNullable(rows.map(_._1)),
        Int32Column.fromNullable(rows.map(_._2)),
      ),
      rows.length,
    )
