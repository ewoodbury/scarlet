package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * `(String, Int) => (String, Int)` over one batch.
 *
 * The function runs on every row in the slice, including a null key. An empty slice does not call
 * it. A result that is not a pair of a string and an int declines before the batch is published.
 * Each column owns its validity words. Each output batch owns its dictionary: the array is not the
 * input dictionary. `""` stays present. `"é"` and `"e\u0301"` stay distinct codes.
 */
class TestStringPairProject extends AnyFlatSpec with Matchers:

  private val accent = "é"
  private val decomposed = "e\u0301"

  "StringPairProject" should "publish an empty slice without calling the function" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 1, "b" -> 2))
    val calls = new AtomicInteger
    val out = StringPairProject.project(batch, 2, 0, counting(calls, keep))
    out.length shouldBe 0
    calls.get() shouldBe 0
    val key = utf8(out, 0)
    val value = intColumn(out, 1)
    key.validity.words should not be theSameInstanceAs(value.validity.words)
    key.dictionary should not be theSameInstanceAs(utf8(batch, 0).dictionary)
    key.dictionary.length shouldBe 0
  }

  it should "map the slice that starts after the first row" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 10, "b" -> 20, "c" -> 30, "d" -> 40))
    val calls = new AtomicInteger
    val out = StringPairProject.project(batch, 1, 2, counting(calls, shift))
    calls.get() shouldBe 2
    out.length shouldBe 2
    cells(out) shouldBe Seq("b!:21", "c!:31")
    utf8(out, 0).dictionary should not be theSameInstanceAs(utf8(batch, 0).dictionary)
  }

  it should "decline a null, a non-pair, a non-string key, and a non-int value" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 10))
    StringPairProject.resetPublishedForTest()
    val messages = Seq(
      constant(nullResult),
      constant("no"),
      constant((1, 2)),
      constant(("a", "b")),
    ).map { mapper =>
        val thrown = intercept[ColumnarFallback] {
          StringPairProject.project(batch, 0, 1, mapper)
        }
        thrown.getMessage
      }
    messages.foreach(_.startsWith("pair map did not stay (String, Int)") shouldBe true)
    StringPairProject.publishedForTest shouldBe 0
  }

  it should "call the function on a null key, a null value, and both" in {
    val batch = nullablePairs(
      Seq(
        (Some("a"), Some(10)),
        (None, Some(20)),
        (Some("c"), None),
        (None, None),
      ),
    )
    val calls = new AtomicInteger
    val out = StringPairProject.project(batch, 0, 4, adaptCounted(calls, rewriteNull))
    calls.get() shouldBe 4
    cells(out) shouldBe Seq("a!:10", "gone:20", "c!:null", "gone:null")
  }

  it should "keep an empty key at code 0 and a null key absent at the same code" in {
    val batch = nullablePairs(Seq((Some(""), Some(1)), (None, Some(2))))
    val out = StringPairProject.project(batch, 0, 2, counting(new AtomicInteger, keep))
    val key = utf8(out, 0)
    key.isValid(0) shouldBe true
    key.isValid(1) shouldBe false
    key.codes(0) shouldBe 0
    key.codes(1) shouldBe 0
    key.dictionary.toSeq shouldBe Seq("")
    cells(out) shouldBe Seq(":1", "null:2")
  }

  it should "keep the two accented forms as distinct dictionary codes" in {
    val batch = BatchCodec.stringIntPairs(Seq(accent -> 1, decomposed -> 2, "" -> 3))
    val out = StringPairProject.project(batch, 0, 3, counting(new AtomicInteger, keep))
    val dictionary = utf8(out, 0).dictionary
    dictionary.toSeq shouldBe Seq(accent, decomposed, "")
    accent should not be decomposed
    dictionary.indexOf(accent) should not be dictionary.indexOf(decomposed)
    cells(out) shouldBe Seq(s"$accent:1", s"$decomposed:2", ":3")
  }

  it should "give a null result key its own validity words and its own dictionary" in {
    val length = 3
    val held = Validity.allValid(length)
    val before = held.words.clone()
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 10, "b" -> 20, "c" -> 30))
    val out = StringPairProject.project(batch, 0, length, dropKey("b"))
    val key = utf8(out, 0)
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
    key.dictionary should not be theSameInstanceAs(utf8(batch, 0).dictionary)
    held.words.toSeq shouldBe before.toSeq
    cells(out) shouldBe Seq("a:10", "null:20", "c:30")
    out.schema shouldBe batch.schema
  }

  private def keep(key: String, value: Int): (String, Int) = (key, value)

  private def shift(key: String, value: Int): (String, Int) = (key + "!", value + 1)

  /** A null key becomes `gone`. A present key gains `!`. A null value stays null. */
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
      case null => erased("gone", value)
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
        if (key == dropped) then erased(null, Int.box(value)) else (key, value)
      def applyNullValue(key: String): Any = apply(key, 0)
      def applyNullKey(value: Int): Any = apply("", value)
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
  private def utf8(batch: ColumnBatch, ordinal: Int): DictUtf8Column =
    batch.columns(ordinal) match
      case column: DictUtf8Column => column
      case other => fail(s"column $ordinal was ${other.getClass.getName}")

  @SuppressWarnings(Array("org.wartremover.warts.SeqApply"))
  private def intColumn(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns(ordinal) match
      case column: Int32Column => column
      case other => fail(s"column $ordinal was ${other.getClass.getName}")

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def cells(batch: ColumnBatch): Seq[String] =
    BatchCodec.decodeStringPairs(batch, batch.length).map { row =>
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
