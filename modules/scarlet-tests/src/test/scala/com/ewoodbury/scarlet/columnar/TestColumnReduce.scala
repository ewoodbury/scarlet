package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestColumnReduce extends AnyFlatSpec with Matchers:

  "count" should "include null slots" in {
    val batch = BatchCodec.nullableInts(Seq(Some(5), None, Some(7)))

    ColumnReduce.count(Vector(batch)) shouldBe 3L
  }

  "sumInt32" should "skip nulls and add a fully present column as a long" in {
    val sparse = BatchCodec.nullableInts(Seq(Some(Int.MaxValue), None, Some(1)))
    val dense = BatchCodec.ints(Seq(Int.MaxValue, 1))

    ColumnReduce.sumInt32(Vector(sparse)) shouldBe Int.MaxValue.toLong + 1L
    ColumnReduce.sumInt32(Vector(dense)) shouldBe Int.MaxValue.toLong + 1L
    ColumnReduce.sumInt32(Vector.empty) shouldBe 0L
  }

  "sumFloat64" should "skip nulls" in {
    val batch = BatchCodec.nullableFloat64s(Seq(Some(1.5), None, Some(2.5)))

    ColumnReduce.sumFloat64(Vector(batch)) shouldBe 4.0
    ColumnReduce.count(Vector(batch)) shouldBe 3L
  }
