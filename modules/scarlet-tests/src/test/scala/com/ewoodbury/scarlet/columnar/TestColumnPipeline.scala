package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.ScarletConf

class TestColumnPipeline extends AnyFlatSpec with Matchers:

  "sum" should "merge partials across morsels without wrapping at Int" in {
    val batches = Vector(
      BatchCodec.intPairs(Seq((1, 2000000000))),
      BatchCodec.intPairs(Seq((1, 2000000000), (2, 5))),
      BatchCodec.intPairs(Seq((Int.MinValue, -1), (2, 4), (3, 1))),
    )

    val once = sums(runSum(batches, parallelism = 1))
    val parallel = sums(runSum(batches, parallelism = 4))

    once shouldBe Map(
      1 -> Some(4000000000L),
      2 -> Some(9L),
      3 -> Some(1L),
      Int.MinValue -> Some(-1L),
    )
    parallel shouldBe once
  }

  it should "keep a valued partial beside a null partial, and a null sum when every partial is null" in {
    val batches = Vector(
      BatchCodec.nullableIntPairs(Seq((Some(1), Some(7)), (Some(2), None))),
      BatchCodec.nullableIntPairs(Seq((Some(1), None), (Some(2), None), (Some(5), None))),
      BatchCodec.nullableIntPairs(Seq((Some(5), Some(9)), (Some(3), Some(4)))),
    )

    val once = sums(runSum(batches, parallelism = 1), requireValues = false)
    val parallel = sums(runSum(batches, parallelism = 4), requireValues = false)

    once shouldBe Map(1 -> Some(7L), 2 -> None, 5 -> Some(9L), 3 -> Some(4L))
    parallel shouldBe once
  }

  it should "merge a null partial with a valued partial inside one batch" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq(
        (Some(1), Some(7)),
        (Some(2), Some(1)),
        (Some(1), None),
        (Some(2), None),
        (Some(3), None),
        (Some(3), None),
      ),
    )

    val once = sums(runSum(Vector(batch), parallelism = 1), requireValues = false)
    val parallel = sums(runSum(Vector(batch), parallelism = 4), requireValues = false)

    once shouldBe Map(1 -> Some(7L), 2 -> Some(1L), 3 -> None)
    parallel shouldBe once
  }

  it should "meet a null partial and a valued partial in one bucket" in {
    val batches = Vector(
      BatchCodec.nullableIntPairs(Seq((Some(1), None), (Some(2), None))),
      BatchCodec.nullableIntPairs(Seq((Some(1), Some(7)), (Some(2), None))),
    )

    val once = sums(runSum(batches, parallelism = 1), requireValues = false)
    val parallel = sums(runSum(batches, parallelism = 4), requireValues = false)

    once shouldBe Map(1 -> Some(7L), 2 -> None)
    parallel shouldBe once
  }

  it should "return one empty batch per partition when the input is empty" in {
    val parts = ColumnPipeline.sumByKey(
      Vector.empty,
      keyOrdinal = 0,
      valueOrdinal = 1,
      numPartitions = 4,
      parallelism = 2,
      batchSize = 8,
    )

    parts.length shouldBe 4
    parts.map(_.length) shouldBe Seq(0, 0, 0, 0)
  }

  it should "keep an output bucket per partition, including empty ones" in {
    val batches = Vector(BatchCodec.intPairs(Seq((1, 4), (1, 1))))
    val parts = runSum(batches, parallelism = 2)

    parts.length shouldBe 4
    parts.map(_.length).sum shouldBe 1
    sums(parts) shouldBe Map(1 -> Some(5L))
  }

  "inner join" should "match one batch join after the exchange" in {
    val build = Vector(
      BatchCodec.intPairs(Seq((1, 10), (1, 11), (4, 40))),
      BatchCodec.intPairs(Seq((2, 20), (Int.MinValue, 7))),
    )
    val probe = Vector(
      BatchCodec.intPairs(Seq((1, 100), (3, 30))),
      BatchCodec.intPairs(Seq((2, 200), (1, 101), (Int.MinValue, 8))),
    )
    val combinedBuild = ColumnBatches.concat(build)
    val combinedProbe = ColumnBatches.concat(probe)
    val single = HashJoin.innerInt32(
      HashJoin.JoinSide(combinedBuild, key = 0, value = 1),
      HashJoin.JoinSide(combinedProbe, key = 0, value = 1),
    )
    val parts = ColumnPipeline.innerJoin(
      build,
      probe,
      numPartitions = 4,
      parallelism = 4,
      batchSize = 2,
    )

    parts.length shouldBe 4
    triples(parts).sorted shouldBe triples(Vector(single)).sorted
    parts.zipWithIndex.foreach { (part, bucket) =>
      int32(part, 0).values.take(part.length).foreach { key =>
        Math.floorMod(key, 4) shouldBe bucket
      }
    }
  }

  it should "emit an empty partition when one side is empty" in {
    val build = Vector(BatchCodec.intPairs(Seq((1, 10))))
    val probe = Vector(BatchCodec.intPairs(Seq.empty[(Int, Int)]))
    val parts = ColumnPipeline.innerJoin(
      build,
      probe,
      numPartitions = 4,
      parallelism = 2,
      batchSize = 8,
    )

    parts.length shouldBe 4
    parts.map(_.length) shouldBe Seq(0, 0, 0, 0)
  }

  "broadcast join" should "match the hash join for unique, duplicate, empty, and hot keys" in {
    val cases = Seq(
      "unique" -> (
        Seq((1, 10), (2, 20), (4, 40), (Int.MinValue, 7)),
        Seq((1, 100), (3, 30), (2, 200), (Int.MinValue, 8)),
      ),
      "duplicate" -> (
        Seq((1, 1), (1, 2), (2, 3), (2, 4), (1, 5)),
        Seq((1, 10), (1, 11), (2, 20), (3, 30), (2, 21)),
      ),
      "empty-build" -> (Seq.empty[(Int, Int)], Seq((1, 10), (2, 20))),
      "empty-probe" -> (Seq((1, 10), (2, 20)), Seq.empty[(Int, Int)]),
      "hot-key" -> (
        (0 until 6).map(row => (0, row)) ++ Seq((1, 7)),
        (0 until 4).map(row => (0, row + 10)) ++ Seq((2, 8)),
      ),
    )
    cases.foreach { case (label, (leftRows, rightRows)) =>
      withClue(s"$label: ") {
        val left = batchesOf(leftRows)
        val right = batchesOf(rightRows)
        val hashed = ColumnPipeline.innerJoin(left, right, 4, parallelism = 1, batchSize = 2)
        val once = broadcast(left, right, parallelism = 1)
        val parallel = broadcast(left, right, parallelism = 4)
        triples(once).sorted shouldBe triples(hashed).sorted
        triples(parallel).sorted shouldBe triples(hashed).sorted
      }
    }
  }

  it should "drop null keys on either side and preserve a null value" in {
    val build = Vector(
      BatchCodec.nullableIntPairs(
        Seq((None, Some(99)), (Some(1), None), (Some(1), Some(10))),
      ),
    )
    val probe = Vector(
      BatchCodec.nullableIntPairs(Seq((None, Some(7)), (Some(1), Some(100)), (Some(3), None))),
    )
    val hashed = ColumnPipeline.innerJoin(probe, build, 4, parallelism = 1, batchSize = 8)
    val joined = broadcast(probe, build, parallelism = 1)
    // Hash join builds the left input. Broadcast builds the right input. Both publish
    // (key, left, right), so the null build value is the right value here.
    nullableTriples(joined).sorted shouldBe Seq((1, Some(100), None), (1, Some(100), Some(10)))
    nullableTriples(joined).sorted shouldBe nullableTriples(hashed).sorted

    val nullBuild = Vector(BatchCodec.nullableIntPairs(Seq((None, Some(1)), (None, Some(2)))))
    val presentProbe = Vector(BatchCodec.intPairs(Seq((1, 3))))
    broadcast(presentProbe, nullBuild, parallelism = 1).map(_.length) shouldBe Seq(0)
    val nullProbe = Vector(BatchCodec.nullableIntPairs(Seq((None, Some(3)))))
    broadcast(nullProbe, presentProbe, parallelism = 1).map(_.length) shouldBe Seq(0)
  }

  it should "publish one batch per probe batch, with an unspecified match order" in {
    val build = Vector(
      BatchCodec.intPairs(Seq((1, 10), (2, 20))),
      BatchCodec.intPairs(Seq((1, 11))),
    )
    val probe = Vector(
      BatchCodec.intPairs(Seq((1, 100), (2, 200))),
      BatchCodec.intPairs(Seq((1, 101))),
    )
    val parts = broadcast(probe, build, parallelism = 4)
    parts.length shouldBe 2
    val first = parts.headOption.getOrElse(fail("missing first probe batch"))
    val second = parts.drop(1).headOption.getOrElse(fail("missing second probe batch"))
    triples(Vector(first)).sorted shouldBe Seq((1, 100, 10), (1, 100, 11), (2, 200, 20)).sorted
    triples(Vector(second)).sorted shouldBe Seq((1, 101, 10), (1, 101, 11)).sorted

    val combinedBuild = ColumnBatches.concat(build)
    val combinedProbe = ColumnBatches.concat(probe)
    val single = HashJoin.innerInt32(
      HashJoin.JoinSide(combinedBuild, key = 0, value = 1),
      HashJoin.JoinSide(combinedProbe, key = 0, value = 1),
    )
    val opened = HashJoin
      .openInt32(
        build.map(batch => HashJoin.JoinSide(batch, key = 0, value = 1)),
        buildBudget,
      )
      .probe(HashJoin.JoinSide(combinedProbe, key = 0, value = 1))
    // innerInt32 writes (key, build, probe). The multi-batch probe writes (key, probe, build).
    swapped(triples(Vector(single))).sorted shouldBe triples(Vector(opened)).sorted

    broadcast(Vector.empty, build, parallelism = 1) shouldBe Vector.empty[ColumnBatch]
    val emptyBuild = broadcast(probe, Vector.empty, parallelism = 1)
    emptyBuild.length shouldBe 2
    emptyBuild.map(_.length) shouldBe Seq(0, 0)
  }

  it should "refuse a build table above the budget and still build one that fits" in {
    val build = Vector(
      BatchCodec.intPairs(Seq((1, 10))),
      BatchCodec.intPairs(Seq((1, 11), (2, 20))),
    )
    val probe = Vector(BatchCodec.intPairs(Seq((1, 100))))
    val rows = 3
    val estimate = HashJoin.retainedBuildBytes(rows)
    estimate shouldBe 183L
    val over = intercept[IllegalStateException] {
      broadcast(probe, build, parallelism = 1, buildBudgetBytes = estimate - 1)
    }
    over.getMessage shouldBe
      s"broadcast build table retains about $estimate bytes for $rows rows, above the budget of ${estimate - 1} bytes"
    triples(broadcast(probe, build, parallelism = 1, buildBudgetBytes = estimate)).sorted shouldBe
      Seq((1, 100, 10), (1, 100, 11)).sorted
    broadcast(probe, Vector.empty, parallelism = 1, buildBudgetBytes = 0L).map(_.length) shouldBe
      Seq(0)
  }

  private def broadcast(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
      parallelism: Int,
      buildBudgetBytes: Long = buildBudget,
  ): Vector[ColumnBatch] =
    ColumnPipeline.broadcastJoin(probe, build, parallelism, buildBudgetBytes)

  private def buildBudget: Long = ScarletConf.get.broadcastBuildBudgetBytes

  private def runSum(batches: Vector[ColumnBatch], parallelism: Int): Vector[ColumnBatch] =
    ColumnPipeline.sumByKey(
      batches,
      keyOrdinal = 0,
      valueOrdinal = 1,
      numPartitions = 4,
      parallelism = parallelism,
      batchSize = 2,
    )

  private def sums(
      batches: Vector[ColumnBatch],
      requireValues: Boolean = true,
  ): Map[Int, Option[Long]] =
    batches.flatMap { batch =>
      val keys = int32(batch, 0)
      val values = int64(batch, 1)
      (0 until batch.length).map { row =>
        require(keys.isValid(row))
        if (requireValues) then require(values.isValid(row))
        val value = if (values.isValid(row)) Some(values.values(row)) else None
        keys.values(row) -> value
      }
    }.toMap

  private def batchesOf(rows: Seq[(Int, Int)]): Vector[ColumnBatch] =
    if (rows.isEmpty) then Vector(BatchCodec.intPairs(Seq.empty[(Int, Int)]))
    else rows.grouped(2).map(group => BatchCodec.intPairs(group)).toVector

  private def triples(batches: Vector[ColumnBatch]): Seq[(Int, Int, Int)] =
    batches.flatMap { batch =>
      val keys = int32(batch, 0)
      val build = int32(batch, 1)
      val probe = int32(batch, 2)
      (0 until batch.length).map { row =>
        (keys.values(row), build.values(row), probe.values(row))
      }
    }

  private def swapped(rows: Seq[(Int, Int, Int)]): Seq[(Int, Int, Int)] =
    rows.map { case (key, build, probe) => (key, probe, build) }

  private def nullableTriples(
      batches: Vector[ColumnBatch],
  ): Seq[(Int, Option[Int], Option[Int])] =
    batches.flatMap { batch =>
      val keys = int32(batch, 0)
      val left = int32(batch, 1)
      val right = int32(batch, 2)
      (0 until batch.length).map { row =>
        val leftValue = if (left.isValid(row)) Some(left.values(row)) else None
        val rightValue = if (right.isValid(row)) Some(right.values(row)) else None
        (keys.values(row), leftValue, rightValue)
      }
    }

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int64Column) => column
      case other => fail(s"expected Int64 at $ordinal, got $other")
