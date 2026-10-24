package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.columnar.ColumnKernel.{Int64Map, Int64Predicate}
import com.ewoodbury.scarlet.columnar.Int64Pipeline.Step

class TestColumnReduce extends AnyFlatSpec with Matchers:

  "count" should "include null slots" in {
    val batch = BatchCodec.nullableInts(Seq(Some(5), None, Some(7)))

    ColumnReduce.count(Vector(batch)) shouldBe 3L
  }

  "sumInt32" should "skip a non-zero hole and ignore spare capacity" in {
    val hole = Array(Int.MaxValue, 9, 1)
    val sparse = Int32Column.of(hole, Validity.pack(hole.length, row => row != 1), hole.length)
    val dense = BatchCodec.ints(Seq(Int.MaxValue, 1))
    val values = Array(5, 7, 99)
    val survivors = Int32Column.of(values, Validity.prefixValid(values.length, 2), 2)

    ColumnReduce.sumInt32(Vector(batch(sparse))) shouldBe Int.MaxValue.toLong + 1L
    ColumnReduce.sumInt32(Vector(dense)) shouldBe Int.MaxValue.toLong + 1L
    ColumnReduce.sumInt32(Vector(batch(survivors))) shouldBe 12L
    ColumnReduce.sumInt32(Vector.empty) shouldBe 0L
  }

  "sumFiltered" should "skip a non-zero hole and add only survivors" in {
    val values = Array(5, 9, 7)
    val column = Int32Column.of(values, Validity.pack(values.length, row => row != 1), values.length)
    val batch = new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

    Int32Pipeline.sumFiltered(batch, _ => true) shouldBe 12L
    Int32Pipeline.sumFP(batch, value => value > 0, value => value * 2) shouldBe 24L
  }

  "sumFloat64" should "skip a non-zero hole and add a dense column" in {
    val hole = Array(1.5, 8.0, 2.5)
    val sparse = Float64Column.of(hole, Validity.pack(hole.length, row => row != 1), hole.length)

    ColumnReduce.sumFloat64(Vector(batch(sparse))) shouldBe 4.0
    ColumnReduce.sumFloat64(Vector(BatchCodec.float64s(Seq(1.5, 2.5)))) shouldBe 4.0
    ColumnReduce.count(Vector(batch(sparse))) shouldBe 3L
  }

  "nullable longs" should "skip null slots in the fused sum and count" in {
    val rows = Seq(Some(5L), None, Some(7L), None)
    val batch = withNullPayload(
      BatchCodec.nullableLongs(rows),
      Seq(1 -> Long.MinValue, 3 -> 99L),
    )
    val present = rows.flatten

    Int64Pipeline.sum(batch, Nil) shouldBe present.sum
    Int64Pipeline.count(batch, Nil) shouldBe rows.length.toLong

    val mappedCalls = new AtomicInteger
    val mapped = Step.Project(rejectNulls(mappedCalls, _ + 1L))
    Int64Pipeline.sum(batch, List(mapped)) shouldBe present.map(_ + 1L).sum
    mappedCalls.get() shouldBe present.length
    mappedCalls.set(0)
    Int64Pipeline.count(batch, List(mapped)) shouldBe rows.length.toLong
    mappedCalls.get() shouldBe present.length

    val predicateCalls = new AtomicInteger
    val keep = Step.Filter(rejectNulls(predicateCalls, _ > 0L))
    Int64Pipeline.sum(batch, List(keep)) shouldBe present.sum
    Int64Pipeline.count(batch, List(keep)) shouldBe present.length.toLong
    predicateCalls.get() shouldBe present.length * 2

    val laterCalls = new AtomicInteger
    val stopped = List(
      Step.Filter(rejectNulls(new AtomicInteger, _ > 6L)),
      Step.Project(rejectNulls(laterCalls, _ + 1L)),
    )
    Int64Pipeline.sum(batch, stopped) shouldBe 8L
    Int64Pipeline.count(batch, stopped) shouldBe 1L
    laterCalls.get() shouldBe 2

    val wrapped = withNullPayload(
      BatchCodec.nullableLongs(Seq(Some(Long.MaxValue), None, Some(1L))),
      Seq(1 -> 42L),
    )
    Int64Pipeline.sum(wrapped, Nil) shouldBe Long.MinValue
  }

  "sumMappedInt32" should "skip null values and not call the mapper on them" in {
    val rows = Seq((Some(1), Some(3)), (Some(2), None), (Some(4), Some(-1)), (Some(5), None))
    val batch = BatchCodec.nullableIntPairs(rows)
    val calls = new AtomicInteger
    val mapper = new ColumnKernel.Int32Map:
      def apply(value: Int): Int =
        calls.incrementAndGet()
        value * 10
    val summed = ColumnReduce.sumMappedInt32(batch, ordinal = 1, mapper)
    val present = BatchCodec.decodeNullableIntPairs(batch).flatMap(_._2)

    summed shouldBe present.map(value => (value * 10).toLong).sum
    calls.get() shouldBe present.length
  }

  private def batch(column: Column): ColumnBatch =
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  /** Writes a non-zero payload into null slots so adding those slots would change the sum. */
  private def withNullPayload(batch: ColumnBatch, holes: Seq[(Int, Long)]): ColumnBatch =
    val column = batch.columns match
      case Seq(stored: Int64Column) => stored
      case _ => fail(s"expected one Int64 column, got ${batch.schema}")
    holes.foreach { (row, payload) =>
      column.values(row) = payload
    }
    Validity.livePresent(column.validity, column.length) shouldBe false
    batch

  private def rejectNulls(calls: AtomicInteger, accept: Long => Long): Int64Map =
    value =>
      calls.incrementAndGet()
      if (value == Long.MinValue || value == 99L || value == 42L) then
        throw new IllegalStateException(s"mapper saw null payload $value")
      accept(value)

  private def rejectNulls(calls: AtomicInteger, accept: Long => Boolean): Int64Predicate =
    value =>
      calls.incrementAndGet()
      if (value == Long.MinValue || value == 99L || value == 42L) then
        throw new IllegalStateException(s"predicate saw null payload $value")
      accept(value)
