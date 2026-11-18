package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * `(Int, Int) => (Int, Int)` over one batch.
 *
 * The function runs on every row in the slice, including a null key. An empty slice does not call
 * it. A result that is not a pair of ints declines before the batch is published. Each column owns
 * its validity words, and [[Validity.allValid]] is not the array those words come from when a cell
 * is null.
 */
class TestIntPairProject extends AnyFlatSpec with Matchers:

  "IntPairProject" should "publish an empty slice without calling the function" in {
    val batch = BatchCodec.intPairs(Seq((1, 10), (2, 20)))
    val calls = new AtomicInteger
    val out = IntPairProject.project(batch, 2, 0, counting(calls, keep))
    out.length shouldBe 0
    calls.get() shouldBe 0
    val key = intColumn(out, 0)
    val value = intColumn(out, 1)
    key.validity.words should not be theSameInstanceAs(value.validity.words)
  }

  it should "map the slice that starts after the first row" in {
    val batch = BatchCodec.intPairs(Seq((1, 10), (2, 20), (3, 30), (4, 40)))
    val calls = new AtomicInteger
    val out = IntPairProject.project(batch, 1, 2, counting(calls, shift))
    calls.get() shouldBe 2
    out.length shouldBe 2
    cells(out) shouldBe Seq("3:21", "4:31")
  }

  it should "decline a null, a non-pair, and a non-int cell" in {
    val batch = BatchCodec.intPairs(Seq((1, 10)))
    val messages = Seq(constant(nullResult), constant("no"), constant((1L, 2))).map { mapper =>
      val thrown = intercept[ColumnarFallback] {
        IntPairProject.project(batch, 0, 1, mapper)
      }
      thrown.getMessage
    }
    messages.foreach(_.startsWith("pair map did not stay (Int, Int)") shouldBe true)
  }

  it should "call the function on a null key, a null value, and both" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq(
        (Some(1), Some(10)),
        (None, Some(20)),
        (Some(3), None),
        (None, None),
      ),
    )
    val calls = new AtomicInteger
    val out = IntPairProject.project(batch, 0, 4, adaptCounted(calls, rewriteNull))
    calls.get() shouldBe 4
    cells(out) shouldBe Seq("2:10", "7:20", "4:null", "7:null")
  }

  it should "give a null result key its own validity words" in {
    val length = 3
    val held = Validity.allValid(length)
    val before = held.words.clone()
    val batch = BatchCodec.intPairs(Seq((1, 10), (2, 20), (3, 30)))
    val out = IntPairProject.project(batch, 0, length, dropKey(2))
    val key = intColumn(out, 0)
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
    cells(out) shouldBe Seq("1:10", "null:20", "3:30")
  }

  private def keep(key: Int, value: Int): (Int, Int) = (key, value)

  private def shift(key: Int, value: Int): (Int, Int) = (key + 1, value + 1)

  /** A null key becomes 7. A present key is incremented. A null value stays null. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Throw",
    ),
  )
  private def rewriteNull(pair: (Int, Int)): (Int, Int) =
    val product = pair.asInstanceOf[Product]
    val value = product.productElement(1)
    product.productElement(0) match
      case null => erased(7, value)
      case present: Int => erased(present + 1, value)
      case other => throw new IllegalArgumentException(s"pair map saw $other")

  /** Dense input. Key `dropped` becomes null. The value stays. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def dropKey(dropped: Int): IntPairProject.IntPairMap =
    new IntPairProject.IntPairMap:
      def apply(key: Int, value: Int): Any =
        if (key == dropped) then erased(null, Int.box(value)) else (key, value)
      def applyNullValue(key: Int): Any = apply(key, 0)
      def applyNullKey(value: Int): Any = apply(0, value)
      def applyNulls(): Any = erased(null, null)

  /** Passes the pair through. `pair._1` would unbox a null key before [[rewriteNull]] sees it. */
  private def adaptCounted(
      calls: AtomicInteger,
      fn: ((Int, Int)) => (Int, Int),
  ): IntPairProject.IntPairMap =
    IntPairProject.adapt((pair: (Int, Int)) => {
      calls.incrementAndGet()
      fn(pair)
    })

  private def counting(
      calls: AtomicInteger,
      fn: (Int, Int) => (Int, Int),
  ): IntPairProject.IntPairMap =
    IntPairProject.adapt((pair: (Int, Int)) => {
      calls.incrementAndGet()
      fn(pair._1, pair._2)
    })

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullResult: Any = null

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def constant(result: Any): IntPairProject.IntPairMap =
    new IntPairProject.IntPairMap:
      def apply(key: Int, value: Int): Any = result
      def applyNullValue(key: Int): Any = result
      def applyNullKey(value: Int): Any = result
      def applyNulls(): Any = result

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erased(key: Any, value: Any): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.SeqApply"))
  private def intColumn(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns(ordinal) match
      case column: Int32Column => column
      case other => fail(s"column $ordinal was ${other.getClass.getName}")

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def cells(batch: ColumnBatch): Seq[String] =
    BatchCodec.decodeIntPairsPreservingAbsence(batch, batch.length).map { row =>
      val product = row.asInstanceOf[Product]
      String.valueOf(product.productElement(0)) + ":" + String.valueOf(product.productElement(1))
    }
