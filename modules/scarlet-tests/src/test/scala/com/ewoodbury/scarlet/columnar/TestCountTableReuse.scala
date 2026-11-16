package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A stripe table accumulates one batch, then forgets those keys before the next batch. */
class TestCountTableReuse extends AnyFlatSpec with Matchers:

  private val keep: ColumnKernel.Int32Predicate = _ => true

  "FoldedCount" should "fold every morsel of one batch into a single table" in {
    FoldedCount.resetReuseForTest()
    HashCount.resetReuseForTest()
    val steps = List(Selection.Step.Int32(1, keep))
    val batch = BatchCodec.intPairs(Array(1, 2, 1, 2), Array(3, 4, 5, 6))
    val counted = FoldedCount
      .count(Vector(batch), steps, numPartitions = 1, parallelism = 1, batchSize = 2)
      .getOrElse(fail("folded count declined"))

    counts(counted) shouldBe Map(1 -> 2, 2 -> 2)
    FoldedCount.tablesAllocatedForTest shouldBe 1L
    FoldedCount.tablesReusedForTest shouldBe 0L
    HashCount.tablesAllocatedForTest shouldBe 1L
    HashCount.tablesReusedForTest shouldBe 0L
  }

  it should "clear the partial table between source batches" in {
    FoldedCount.resetReuseForTest()
    HashCount.resetReuseForTest()
    val steps = List(Selection.Step.Int32(1, keep))
    val first = BatchCodec.intPairs(Array(1, 1), Array(3, 4))
    val second = BatchCodec.intPairs(Array(1, 2), Array(5, 6))
    val counted = FoldedCount
      .count(Vector(first, second), steps, numPartitions = 1, parallelism = 1, batchSize = 8)
      .getOrElse(fail("folded count declined"))

    counts(counted) shouldBe Map(1 -> 3, 2 -> 1)
    FoldedCount.tablesAllocatedForTest shouldBe 1L
    FoldedCount.tablesReusedForTest shouldBe 1L
    HashCount.tablesAllocatedForTest shouldBe 1L
    HashCount.tablesReusedForTest shouldBe 0L
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
