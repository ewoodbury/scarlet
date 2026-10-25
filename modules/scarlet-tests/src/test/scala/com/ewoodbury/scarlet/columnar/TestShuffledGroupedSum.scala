package com.ewoodbury.scarlet.columnar

import scala.util.Random

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Shuffled and boundary inputs for the direct-index grouped sum.
 *
 * The index emits ascending keys. The hash table emits first-seen order. Sums are compared as
 * maps. A long sum of two `Int.MaxValue` values must not wrap.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class TestShuffledGroupedSum extends AnyFlatSpec with Matchers:

  "shuffled keys" should "sum duplicates in ascending order" in {
    val width = 256
    val keys = Array.tabulate(width)(row => row % 16)
    shuffle(keys, seed = 58L)
    val values = Array.tabulate(width)(row => row - 100)
    val batch = BatchCodec.intPairs(keys, values)
    val hashed = HashAggregate.sumInt32(batch, 0, 1)
    val indexed = partial(batch)

    sums(indexed) shouldBe sums(hashed)
    keysOf(indexed) shouldBe (0 until 16)
    keysOf(hashed) should not be keysOf(indexed)
    sums(indexed).size shouldBe 16
  }

  "dense keys" should "index a small domain and keep a single group" in {
    val small = BatchCodec.intPairs(Seq((2, 5), (0, 1), (2, -3), (1, 4), (0, 2)))
    val indexed = partial(small)
    val hashed = HashAggregate.sumInt32(small, 0, 1)

    sums(indexed) shouldBe sums(hashed)
    sums(indexed) shouldBe Map(0 -> Some(3L), 1 -> Some(4L), 2 -> Some(2L))
    keysOf(indexed) shouldBe Seq(0, 1, 2)
    keysOf(hashed) shouldBe Seq(2, 0, 1)

    val single = BatchCodec.intPairs(Seq.fill(32)((4, 3)))
    val one = partial(single)

    keysOf(one) shouldBe Seq(4)
    sums(one) shouldBe Map(4 -> Some(96L))
  }

  "the full int domain" should "hash and keep first-seen order without wrapping the sum" in {
    val batch = BatchCodec.intPairs(
      Seq(
        (Int.MaxValue, Int.MaxValue),
        (Int.MinValue, 5),
        (Int.MaxValue, Int.MaxValue),
        (Int.MinValue, -1),
      ),
    )
    val indexed = partial(batch)
    val hashed = HashAggregate.sumInt32(batch, 0, 1)
    val wide = Int.MaxValue.toLong + Int.MaxValue.toLong

    sums(indexed) shouldBe sums(hashed)
    keysOf(indexed) shouldBe Seq(Int.MaxValue, Int.MinValue)
    keysOf(indexed) shouldBe keysOf(hashed)
    sums(indexed) shouldBe Map(Int.MaxValue -> Some(wide), Int.MinValue -> Some(4L))
  }

  it should "not wrap a dense sum of two max ints" in {
    val batch = BatchCodec.intPairs(Seq((6, Int.MaxValue), (6, Int.MaxValue), (6, 2)))
    val indexed = partial(batch)

    keysOf(indexed) shouldBe Seq(6)
    sums(indexed) shouldBe Map(6 -> Some(Int.MaxValue.toLong + Int.MaxValue.toLong + 2L))
  }

  "nulls" should "drop a null key and keep a null sum" in {
    val rows = Seq(
      (None, Some(9)),
      (Some(3), None),
      (Some(1), Some(4)),
      (Some(8), None),
      (Some(3), Some(5)),
      (Some(1), None),
      (None, None),
    )
    val batch = BatchCodec.nullableIntPairs(rows)
    val indexed = partial(batch)
    val hashed = HashAggregate.sumInt32(batch, 0, 1)

    sums(indexed) shouldBe sums(hashed)
    sums(indexed) shouldBe Map(1 -> Some(4L), 3 -> Some(5L), 8 -> None)
    keysOf(indexed) shouldBe Seq(1, 3, 8)
  }

  "an empty batch" should "publish an empty partial" in {
    val batch = BatchCodec.intPairs(Seq.empty[(Int, Int)])
    val indexed = partial(batch)
    val hashed = HashAggregate.sumInt32(batch, 0, 1)

    indexed.length shouldBe 0
    hashed.length shouldBe 0
    indexed.schema shouldBe hashed.schema
  }

  "a wide key" should "stay on the hash path in first-seen order" in {
    val length = 8
    val limit = length * 4
    val inside = BatchCodec.intPairs(
      (0 until length).map(row => if (row == 1) then (limit - 1, 2) else (0, 1)),
    )
    val outside = BatchCodec.intPairs(
      (limit, 2) +: (0, 4) +: Seq.fill(length - 2)((limit, 1)),
    )

    keysOf(partial(inside)) shouldBe Seq(0, limit - 1)
    sums(partial(inside)) shouldBe Map(0 -> Some((length - 1).toLong), (limit - 1) -> Some(2L))
    keysOf(partial(outside)) shouldBe Seq(limit, 0)
    sums(partial(outside)) shouldBe Map(0 -> Some(4L), limit -> Some(2L + (length - 2).toLong))
  }

  "a partial merge" should "size the table from the partial, not the source" in {
    val rows = 100
    val keys = Array.tabulate(rows)(row => row % 4)
    val values = Array.tabulate(rows)(row => row)
    val batch = BatchCodec.intPairs(keys, values)
    val merged = GroupedSumKernel.run(
      Vector(batch),
      keyOrdinal = 0,
      valueOrdinal = 1,
      numPartitions = 1,
      parallelism = 1,
      batchSize = 10,
      steps = Vector.empty[PairStep],
    )

    sums(only(merged)) shouldBe sums(HashAggregate.sumInt32(batch, 0, 1))
    keysOf(only(merged)) shouldBe Seq(0, 1, 2, 3)
    HashAggregate.mergeSumTableCapacity should be < ColumnHash.tableSize(rows)
    HashAggregate.mergeSumTableCapacity shouldBe ColumnHash.tableSize(8)
  }

  private def partial(batch: ColumnBatch): ColumnBatch =
    val (_, morsels) = GroupedSumKernel.partialMorsels(
      Vector(batch),
      keyOrdinal = 0,
      valueOrdinal = 1,
      parallelism = 1,
      batchSize = math.max(batch.length, 1),
      steps = Vector.empty[PairStep],
    )
    morsels match
      case Vector(onlyBatch) => onlyBatch
      case other => fail(s"expected one partial, got ${other.length}")

  private def only(batches: Vector[ColumnBatch]): ColumnBatch =
    batches match
      case Vector(batch) => batch
      case other => fail(s"expected one batch, got ${other.length}")

  private def sums(batch: ColumnBatch): Map[Int, Option[Long]] =
    val keys = int32(batch, 0)
    val values = int64(batch, 1)
    (0 until batch.length).map { row =>
      val value = if (values.isValid(row)) Some(values.values(row)) else None
      keys.values(row) -> value
    }.toMap

  private def keysOf(batch: ColumnBatch): Seq[Int] =
    val keys = int32(batch, 0)
    (0 until batch.length).map(row => keys.values(row))

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int64Column) => column
      case other => fail(s"expected Int64 at $ordinal, got $other")

  private def shuffle(values: Array[Int], seed: Long): Unit =
    val random = new Random(seed)
    var offset = values.length - 1
    while (offset > 0) {
      val swap = random.nextInt(offset + 1)
      val held = values(offset)
      values(offset) = values(swap)
      values(swap) = held
      offset -= 1
    }
