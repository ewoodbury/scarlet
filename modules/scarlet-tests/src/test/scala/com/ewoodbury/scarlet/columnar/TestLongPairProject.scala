package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * `(Long, Long) => (Long, Long)` over one batch.
 *
 * The function runs on every row in the slice, including a null key. An empty slice does not call
 * it. A result that is not a pair of longs declines before the batch is published. Each column
 * owns its validity words.
 */
class TestLongPairProject extends AnyFlatSpec with Matchers:

  "LongPairProject" should "publish an empty slice without calling the function" in {
    val batch = BatchCodec.longPairs(Seq(present(1L, 10L), present(2L, 20L)))
    val calls = new AtomicInteger
    val out = LongPairProject.project(batch, 2, 0, counting(calls, keep))
    out.length shouldBe 0
    calls.get() shouldBe 0
    val key = longColumn(out, 0)
    val value = longColumn(out, 1)
    key.validity.words should not be theSameInstanceAs(value.validity.words)
  }

  it should "map the slice that starts after the first row" in {
    val batch = BatchCodec.longPairs(
      Seq(present(1L, 10L), present(2L, 20L), present(3L, 30L), present(4L, 40L)),
    )
    val calls = new AtomicInteger
    val out = LongPairProject.project(batch, 1, 2, counting(calls, shift))
    calls.get() shouldBe 2
    out.length shouldBe 2
    cells(out) shouldBe Seq("3:21", "4:31")
  }

  it should "decline a null, a non-pair, and a non-long cell" in {
    val batch = BatchCodec.longPairs(Seq(present(1L, 10L)))
    val messages = Seq(constant(nullResult), constant("no"), constant((1, 2))).map { mapper =>
      val thrown = intercept[ColumnarFallback] {
        LongPairProject.project(batch, 0, 1, mapper)
      }
      thrown.getMessage
    }
    messages.foreach(_.startsWith("pair map did not stay (Long, Long)") shouldBe true)
  }

  it should "call the function on a null key, a null value, and both" in {
    val batch = BatchCodec.longPairs(
      Seq(
        present(1L, 10L),
        erased(null, Long.box(20L)),
        erased(Long.box(3L), null),
        erased(null, null),
      ),
    )
    val calls = new AtomicInteger
    val out = LongPairProject.project(batch, 0, 4, adaptCounted(calls, rewriteNull))
    calls.get() shouldBe 4
    cells(out) shouldBe Seq("2:10", "7:20", "4:null", "7:null")
  }

  it should "give a null result key its own validity words" in {
    val length = 3
    val held = Validity.allValid(length)
    val before = held.words.clone()
    val batch = BatchCodec.longPairs(Seq(present(1L, 10L), present(2L, 20L), present(3L, 30L)))
    val out = LongPairProject.project(batch, 0, length, dropKey(2L))
    val key = longColumn(out, 0)
    val value = longColumn(out, 1)
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

  private def present(key: Long, value: Long): (Long, Long) = (key, value)

  private def keep(key: Long, value: Long): (Long, Long) = (key, value)

  private def shift(key: Long, value: Long): (Long, Long) = (key + 1L, value + 1L)

  /** A null key becomes 7. A present key is incremented. A null value stays null. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Throw",
    ),
  )
  private def rewriteNull(pair: (Long, Long)): (Long, Long) =
    val product = pair.asInstanceOf[Product]
    val value = product.productElement(1)
    product.productElement(0) match
      case null => erased(Long.box(7L), value)
      case present: Long => erased(Long.box(present + 1L), value)
      case other => throw new IllegalArgumentException(s"pair map saw $other")

  /** Dense input. Key `dropped` becomes null. The value stays. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def dropKey(dropped: Long): LongPairProject.LongPairMap =
    new LongPairProject.LongPairMap:
      def apply(key: Long, value: Long): Any =
        if (key == dropped) then erased(null, Long.box(value)) else (key, value)
      def applyNullValue(key: Long): Any = apply(key, 0L)
      def applyNullKey(value: Long): Any = apply(0L, value)
      def applyNulls(): Any = erased(null, null)

  private def adaptCounted(
      calls: AtomicInteger,
      fn: ((Long, Long)) => (Long, Long),
  ): LongPairProject.LongPairMap =
    LongPairProject.adapt((pair: (Long, Long)) => {
      calls.incrementAndGet()
      fn(pair)
    })

  private def counting(
      calls: AtomicInteger,
      fn: (Long, Long) => (Long, Long),
  ): LongPairProject.LongPairMap =
    LongPairProject.adapt((pair: (Long, Long)) => {
      calls.incrementAndGet()
      fn(pair._1, pair._2)
    })

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullResult: Any = null

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def constant(result: Any): LongPairProject.LongPairMap =
    new LongPairProject.LongPairMap:
      def apply(key: Long, value: Long): Any = result
      def applyNullValue(key: Long): Any = result
      def applyNullKey(value: Long): Any = result
      def applyNulls(): Any = result

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erased(key: Any, value: Any): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.SeqApply"))
  private def longColumn(batch: ColumnBatch, ordinal: Int): Int64Column =
    batch.columns(ordinal) match
      case column: Int64Column => column
      case other => fail(s"column $ordinal was ${other.getClass.getName}")

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def cells(batch: ColumnBatch): Seq[String] =
    BatchCodec.decodeLongPairs(batch).map { row =>
      val product = row.asInstanceOf[Product]
      String.valueOf(product.productElement(0)) + ":" + String.valueOf(product.productElement(1))
    }
