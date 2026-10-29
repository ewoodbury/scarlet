package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestLongHashReduce extends AnyFlatSpec with Matchers:

  private val plus: LongHashReduce.LongBinOp = (left, right) => left + right

  "byKey" should "return one empty long-pair batch per partition for an empty input" in {
    val parts = LongHashReduce.byKey(
      Vector.empty,
      numPartitions = 4,
      parallelism = 2,
      batchSize = 8,
      plus,
    )

    parts.length shouldBe 4
    parts.foreach { batch =>
      batch.length shouldBe 0
      batch.schema shouldBe Vector(LogicalType.Int64, LogicalType.Int64)
    }
  }

  it should "drop a null key, keep one null value, and unbox a later null to zero" in {
    val keys = Int64Column.of(
      Array(1L, 1L, 7L, 2L, 3L, 3L, 4L, 4L),
      Validity.pack(8, row => row != 2),
      8,
    )
    val values = Int64Column.of(
      Array(10L, 0L, 99L, 0L, 4L, 5L, 0L, 0L),
      Validity.pack(8, row => row != 1 && row != 3 && row != 6 && row != 7),
      8,
    )
    val batch = new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(keys, values),
      8,
    )
    val reduced = LongHashReduce.int64(batch, keyOrdinal = 0, valueOrdinal = 1, plus)
    val decoded = BatchCodec.decodeLongPairs(reduced)

    decoded should contain theSameElementsAs Seq(
      pair(longRef(1L), longRef(10L)),
      pair(longRef(2L), absent),
      pair(longRef(3L), longRef(9L)),
      pair(longRef(4L), longRef(0L)),
    )
    assertValidity(reduced, decoded)
    decoded.exists(_._1 == 7L) shouldBe false
  }

  it should "keep a lone null and unbox a null that meets a present value across morsels" in {
    // Batch size 1 makes each row its own morsel, so partial combine, the exchange, and the
    // bucket reduce all see key 2's missing state. Key 1 is null in one morsel and 5 in another.
    val keys = Int64Column.of(Array(3L, 1L, 2L, 1L), Validity.allValid(4), 4)
    val values = Int64Column.of(
      Array(4L, 0L, 0L, 5L),
      Validity.pack(4, row => row == 0 || row == 3),
      4,
    )
    val batch = new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(keys, values),
      4,
    )
    val combine: LongHashReduce.LongBinOp = (left, right) => left + right + 1L
    val parts = LongHashReduce.byKey(
      Vector(batch),
      numPartitions = 4,
      parallelism = 2,
      batchSize = 1,
      combine,
    )
    val decoded = parts.flatMap(part => BatchCodec.decodeLongPairs(part))

    decoded should contain theSameElementsAs Seq(
      pair(longRef(3L), longRef(4L)),
      pair(longRef(1L), longRef(6L)),
      pair(longRef(2L), absent),
    )
    parts.foreach(part => assertValidity(part, BatchCodec.decodeLongPairs(part)))
  }

  it should "merge morsel partials before the exchange" in {
    val rows = (0 until 8).map(row => (1L, row.toLong + 1L))
    val batch = longPairs(rows)
    val parts = LongHashReduce.byKey(
      Vector(batch),
      numPartitions = 4,
      parallelism = 2,
      batchSize = 2,
      plus,
    )

    parts.map(_.length).sum shouldBe 1
    val values = parts.flatMap(part => BatchCodec.decodeLongPairs(part))
    values shouldBe Seq((1L, (1L to 8L).sum))
    parts.foreach(part => assertValidity(part, BatchCodec.decodeLongPairs(part)))
  }

  private def longPairs(rows: Seq[(Long, Long)]): ColumnBatch =
    val keys = Int64Column(rows.map(_._1))
    val values = Int64Column(rows.map(_._2))
    new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(keys, values),
      rows.length,
    )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def pair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absent: java.lang.Long = null

  private def longRef(value: Long): java.lang.Long = java.lang.Long.valueOf(value)

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
    ),
  )
  private def valueMissing(row: (Long, Long)): Boolean =
    row.productElement(1).asInstanceOf[AnyRef] eq null

  private def assertValidity(batch: ColumnBatch, decoded: Seq[(Long, Long)]): Unit =
    val keyColumn = int64(batch, 0)
    val valueColumn = int64(batch, 1)
    decoded.zipWithIndex.foreach { (row, index) =>
      keyColumn.isValid(index) shouldBe true
      valueColumn.isValid(index) shouldBe !valueMissing(row)
    }

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column = batch.columns.lift(ordinal) match
    case Some(column: Int64Column) => column
    case other => fail(s"expected Int64 at $ordinal, got $other")
