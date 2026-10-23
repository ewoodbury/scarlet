package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
