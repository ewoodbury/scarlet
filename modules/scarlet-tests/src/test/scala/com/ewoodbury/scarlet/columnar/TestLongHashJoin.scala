package com.ewoodbury.scarlet.columnar

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf

class TestLongHashJoin extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val wide = 1L << 32
  private val tripleSchema = Vector(LogicalType.Int64, LogicalType.Int64, LogicalType.Int64)

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ()

  "inner join" should "match the row-path join, including duplicate keys" in {
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val buildRows = Seq((1L, 10L), (1L, 11L), (2L, 20L), (-4L, 1L), (Long.MinValue, 7L), (0L, -1L))
    val probeRows = Seq((1L, 100L), (3L, 30L), (2L, 200L), (1L, 101L), (0L, 5L), (Long.MinValue, 8L))
    val fromEngine = DistCollection(buildRows, 4)
      .join(DistCollection(probeRows, 3))
      .collect()
      .map { case (key, (buildValue, probeValue)) => (key, Some(buildValue), Some(probeValue)) }
      .toSeq
      .sorted

    val joined = join(
      pairs(buildRows.map { case (key, value) => (Some(key), Some(value)) }),
      pairs(probeRows.map { case (key, value) => (Some(key), Some(value)) }),
    )

    joined.schema shouldBe tripleSchema
    sorted(joined) shouldBe fromEngine
  }

  it should "not match keys that share a hash code" in {
    // `Long.hashCode` folds the high half, so these two keys share a code and must stay distinct.
    java.lang.Long.hashCode(1L) shouldBe java.lang.Long.hashCode(wide)
    val buildRows = Seq((1L, 10L), (wide, 11L), (wide, 12L))
    val probeRows = Seq((1L, 100L), (wide, 200L), (1L, 101L))
    val joined = join(
      pairs(buildRows.map { case (key, value) => (Some(key), Some(value)) }),
      pairs(probeRows.map { case (key, value) => (Some(key), Some(value)) }),
    )

    sorted(joined) shouldBe Seq(
      (1L, Some(10L), Some(100L)),
      (1L, Some(10L), Some(101L)),
      (wide, Some(12L), Some(200L)),
      (wide, Some(11L), Some(200L)),
    ).sorted
  }

  it should "walk the build chain newest-first, a kernel detail and not a public contract" in {
    val buildRows = Seq(
      (Some(1L), Some(10L)),
      (Some(2L), Some(20L)),
      (Some(1L), Some(11L)),
      (Some(1L), Some(12L)),
    )
    val probeRows = Seq((Some(2L), Some(200L)), (Some(1L), Some(100L)), (Some(2L), Some(201L)))

    inOrder(join(pairs(buildRows), pairs(probeRows))) shouldBe Seq(
      (2L, Some(20L), Some(200L)),
      (1L, Some(12L), Some(100L)),
      (1L, Some(11L), Some(100L)),
      (1L, Some(10L), Some(100L)),
      (2L, Some(20L), Some(201L)),
    )
  }

  it should "preserve null values and drop null keys" in {
    val buildRows = Seq(
      (Some(1L), Some(10L)),
      (Some(1L), None),
      (None, Some(99L)),
      (Some(0L), Some(1L)),
      (Some(2L), Some(20L)),
    )
    val probeRows = Seq((Some(1L), None), (None, Some(7L)), (Some(0L), Some(5L)), (Some(3L), Some(3L)))
    val expected: Seq[(Long, Option[Long], Option[Long])] = Seq(
      (1L, None, None),
      (1L, Some(10L), None),
      (0L, Some(1L), Some(5L)),
    )

    val joined = join(pairs(buildRows), pairs(probeRows))
    sorted(joined) shouldBe expected.sorted
    sorted(joined).map(_._2) should not contain Some(99L)
    sorted(joined).map(_._3) should not contain Some(7L)
  }

  it should "grow the output and keep value validity past a bitmap word" in {
    val buildRows = (0 until 80).map { row =>
      (Some(1L), if (row == 9) None else Some(row.toLong))
    }
    val joined = join(pairs(buildRows), pairs(Seq((Some(1L), Some(7L)))))

    joined.length shouldBe 80
    joined.schema shouldBe tripleSchema
    val ordered = inOrder(joined)
    ordered.map(_._3).distinct shouldBe Seq(Some(7L))
    ordered.map(_._2) shouldBe (79 to 0 by -1).map { row =>
      if (row == 9) None else Some(row.toLong)
    }
    // Newest-first puts build row 9, the null value, at output index 70, past the first word.
    int64(joined, 0).isValid(70) shouldBe true
    int64(joined, 1).isValid(64) shouldBe true
    int64(joined, 1).isValid(70) shouldBe false
  }

  it should "return an empty batch when a side is empty or nothing matches" in {
    val build = pairs(Seq((Some(1L), Some(1L))))
    val probe = pairs(Seq((Some(2L), Some(2L))))
    val empty = pairs(Seq.empty)

    join(empty, build).length shouldBe 0
    join(build, empty).length shouldBe 0
    join(empty, empty).schema shouldBe tripleSchema
    sorted(join(build, probe)) shouldBe Seq.empty[(Long, Option[Long], Option[Long])]
  }

  it should "not read past the live length" in {
    val buildKeys = Int64Column.of(Array(1L, 1L), Validity.allValid(2), 2)
    val buildValues = Int64Column.of(Array(10L, 11L), Validity.allValid(2), 2)
    val probeKeys = Int64Column.of(Array(1L, 1L), Validity.allValid(2), length = 1)
    val probeValues = Int64Column.of(Array(100L, 999L), Validity.allValid(2), length = 1)
    val build = new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(buildKeys, buildValues),
      2,
    )
    val probe = new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(probeKeys, probeValues),
      1,
    )

    val joined = join(build, probe)
    joined.length shouldBe 2
    inOrder(joined).map(_._2) shouldBe Seq(Some(11L), Some(10L))
    inOrder(joined).map(_._3) shouldBe Seq(Some(100L), Some(100L))
  }

  it should "reject a bad ordinal or a non-long key" in {
    val batch = pairs(Seq((Some(1L), Some(1L))))

    an[IllegalArgumentException] should be thrownBy {
      LongHashJoin.innerInt64(
        HashJoin.JoinSide(batch, key = 2, value = 0),
        HashJoin.JoinSide(batch, key = 0, value = 1),
      )
    }
    an[IllegalArgumentException] should be thrownBy {
      LongHashJoin.innerInt64(
        HashJoin.JoinSide(pairs(Seq.empty), key = 2, value = 0),
        HashJoin.JoinSide(batch, key = 0, value = 1),
      )
    }
    an[IllegalArgumentException] should be thrownBy {
      LongHashJoin.innerInt64(
        HashJoin.JoinSide(
          new ColumnBatch(Vector(LogicalType.Int32), Vector(Int32Column(Seq(1))), 1),
          key = 0,
          value = 0,
        ),
        HashJoin.JoinSide(batch, key = 0, value = 1),
      )
    }
  }

  it should "join co-partitioned buckets across four partitions" in {
    val build = BatchCodec.longPairs(Seq((1L, 10L), (wide, 11L), (-4L, 20L), (Long.MinValue, 7L)))
    val probe = BatchCodec.longPairs(Seq((1L, 100L), (wide, 200L), (-4L, 30L), (5L, 40L)))
    val parts = ColumnPipeline.longInnerJoin(
      Vector(build),
      Vector(probe),
      numPartitions = 4,
      parallelism = 2,
      batchSize = 2,
    )

    parts.length shouldBe 4
    parts.zipWithIndex.foreach { (batch, index) =>
      batch.schema shouldBe tripleSchema
      val decoded = BatchCodec.decodeLongTriples(batch)
      decoded.length shouldBe batch.length
      (0 until 3).foreach { ordinal =>
        val column = int64(batch, ordinal)
        (0 until batch.length).foreach(row => column.isValid(row) shouldBe true)
      }
      decoded.foreach { case (key, _) =>
        Math.floorMod(java.lang.Long.hashCode(key), parts.length) shouldBe index
      }
    }
    val rows = parts.flatMap(batch => BatchCodec.decodeLongTriples(batch))
    rows should contain theSameElementsAs Seq(
      (1L, (10L, 100L)),
      (wide, (11L, 200L)),
      (-4L, (20L, 30L)),
    )
    Math.floorMod(java.lang.Long.hashCode(1L), 4) shouldBe Math.floorMod(java.lang.Long.hashCode(wide), 4)
  }

  private def join(build: ColumnBatch, probe: ColumnBatch): ColumnBatch =
    val batch = LongHashJoin.innerInt64(
      HashJoin.JoinSide(build, key = 0, value = 1),
      HashJoin.JoinSide(probe, key = 0, value = 1),
    )
    batch.schema shouldBe tripleSchema
    batch

  private def pairs(rows: Seq[(Option[Long], Option[Long])]): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int64Column.fromNullable(rows.map(_._1)),
        Int64Column.fromNullable(rows.map(_._2)),
      ),
      rows.size,
    )

  private def sorted(batch: ColumnBatch): Seq[(Long, Option[Long], Option[Long])] =
    inOrder(batch).sorted

  private def inOrder(batch: ColumnBatch): Seq[(Long, Option[Long], Option[Long])] =
    val keys = int64(batch, 0)
    val buildValues = int64(batch, 1)
    val probeValues = int64(batch, 2)
    (0 until batch.length).map { row =>
      val buildValue = if (buildValues.isValid(row)) Some(buildValues.values(row)) else None
      val probeValue = if (probeValues.isValid(row)) Some(probeValues.values(row)) else None
      (keys.values(row), buildValue, probeValue)
    }

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int64Column) => column
      case other => fail(s"expected Int64 at $ordinal, got $other")
