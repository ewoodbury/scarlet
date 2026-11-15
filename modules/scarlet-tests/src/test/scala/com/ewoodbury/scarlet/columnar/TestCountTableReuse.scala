package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A reused count table must not keep keys from the morsel it just cleared. */
class TestCountTableReuse extends AnyFlatSpec with Matchers:

  private val keep: ColumnKernel.Int32Predicate = _ => true

  "FoldedCount" should "reuse one partial table and forget the previous morsel's keys" in {
    FoldedCount.resetReuseForTest()
    HashCount.resetReuseForTest()
    val steps = List(Selection.Step.Int32(1, keep))
    val batch = BatchCodec.intPairs(Array(1, 1, 2, 2), Array(3, 4, 5, 6))
    val counted = FoldedCount
      .count(Vector(batch), steps, numPartitions = 1, parallelism = 1, batchSize = 2)
      .getOrElse(fail("folded count declined"))

    counts(counted) shouldBe Map(1 -> 2, 2 -> 2)
    FoldedCount.tablesAllocatedForTest shouldBe 1L
    FoldedCount.tablesReusedForTest shouldBe 1L
    HashCount.tablesAllocatedForTest shouldBe 1L
    HashCount.tablesReusedForTest shouldBe 1L
  }

  "HashCount" should "reuse one table across two batches and publish only the second" in {
    HashCount.resetReuseForTest()
    val first = counts(Vector(HashCount.int32(BatchCodec.intPairs(Array(1, 1), Array(0, 0)))))
    val second = counts(Vector(HashCount.int32(BatchCodec.intPairs(Array(2, 2), Array(0, 0)))))

    first shouldBe Map(1 -> 2)
    second shouldBe Map(2 -> 2)
    HashCount.tablesAllocatedForTest shouldBe 1L
    HashCount.tablesReusedForTest shouldBe 1L
  }

  private def counts(batches: Vector[ColumnBatch]): Map[Int, Int] =
    batches.flatMap(batch => BatchCodec.decodeIntPairs(batch)).toMap
