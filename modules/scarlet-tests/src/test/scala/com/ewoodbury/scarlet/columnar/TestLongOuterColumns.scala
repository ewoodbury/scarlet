package com.ewoodbury.scarlet.columnar

import java.nio.file.{Files, Path}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Absence on an `Int64` column: decode, slice, concat, and the spill tag. */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TestLongOuterColumns extends AnyFlatSpec with Matchers:

  "long outer decode" should "read an absence bit as None when the payload is zero" in {
    val right = Int64Column.withAbsence(
      Array(0L),
      Validity.pack(1, _ => false),
      Validity.pack(1, _ => true),
      1,
    )
    val batch = triple(present(Array(1L)), present(Array(10L)), right)
    right.isAbsent(0) shouldBe true
    right.isValid(0) shouldBe false
    LongOuterDecode.decode(batch, AbsentSide.Right) shouldBe Seq((1L, (10L, None)))
    val thrown = intercept[IllegalArgumentException] {
      val _ = right.isAbsent(1)
    }
    thrown.getMessage should include("outside column length")
  }

  it should "reject a clear bit that is not an absence mark" in {
    val right = Int64Column.of(Array(0L), Validity.pack(1, _ => false), 1)
    val batch = triple(present(Array(1L)), present(Array(10L)), right)
    right.absence shouldBe None
    val thrown = intercept[IllegalArgumentException] {
      val _ = LongOuterDecode.decode(batch, AbsentSide.Right)
    }
    thrown.getMessage should include("null Int64 at row 0 on an optional outer column")
  }

  it should "keep a clear key bit as null" in {
    val keys = Int64Column.of(Array(0L), Validity.pack(1, _ => false), 1)
    val batch = triple(keys, present(Array(10L)), present(Array(20L)))
    val decoded = LongOuterDecode.decode(batch, AbsentSide.Right)
    decoded.length shouldBe 1
    nullKey(decoded.head) shouldBe true
  }

  "long absence" should "survive slice, concat, and a spill round trip" in {
    val marked = Int64Column.withAbsence(
      Array(1L, 2L),
      Validity.allValid(2),
      Validity.pack(2, row => row == 0),
      2,
    )
    val batch = new ColumnBatch(Vector(LogicalType.Int64), Vector(marked), 2)
    val pieces = ColumnBatches.slice(batch, 1)
    pieces.length shouldBe 2
    int64(pieces(0)).isAbsent(0) shouldBe true
    int64(pieces(1)).isAbsent(0) shouldBe false
    val plain = Int64Column.of(Array(3L), Validity.allValid(1), 1)
    val plainBatch = new ColumnBatch(Vector(LogicalType.Int64), Vector(plain), 1)
    val joined = ColumnBatches.concat(Vector(pieces(0), plainBatch))
    int64(joined).isAbsent(0) shouldBe true
    int64(joined).isAbsent(1) shouldBe false
    int64(joined).isValid(0) shouldBe true
    withDir { dir =>
      SpillStore.withSpill(dir) { store =>
        val read = store.write(Vector(batch)).read()
        read.length shouldBe 1
        val column = int64(read.head)
        column.isAbsent(0) shouldBe true
        column.isAbsent(1) shouldBe false
        column.isValid(0) shouldBe true
        column.isValid(1) shouldBe true
        column.values(0) shouldBe 1L
        column.values(1) shouldBe 2L
        val plainRead = store.write(Vector(plainBatch)).read()
        int64(plainRead.head).absence shouldBe None
      }
    }
    SpillLayout.Version shouldBe 2
  }

  it should "write a zero absence bitmap for a plain batch in a tag 4 file" in {
    val absent = Int64Column.withAbsence(
      Array(0L),
      Validity.pack(1, _ => false),
      Validity.pack(1, _ => true),
      1,
    )
    val plain = present(Array(2L))
    val absentBatch = new ColumnBatch(Vector(LogicalType.Int64), Vector(absent), 1)
    val plainBatch = new ColumnBatch(Vector(LogicalType.Int64), Vector(plain), 1)
    withDir { dir =>
      SpillStore.withSpill(dir) { store =>
        val read = store.write(Vector(absentBatch, plainBatch)).read()
        read.length shouldBe 2
        int64(read.head).isAbsent(0) shouldBe true
        int64(read.head).isValid(0) shouldBe false
        int64(read(1)).absence shouldBe None
        int64(read(1)).values(0) shouldBe 2L
      }
    }
  }

  it should "refuse to stream an absent Int64 column" in {
    val absent = Int64Column.withAbsence(
      Array(0L),
      Validity.pack(1, _ => false),
      Validity.pack(1, _ => true),
      1,
    )
    val batch = new ColumnBatch(Vector(LogicalType.Int64), Vector(absent), 1)
    withDir { dir =>
      val thrown = intercept[IllegalArgumentException] {
        SpillStore.withSpill(dir) { store =>
          val _ = store.writeStreaming(Vector(LogicalType.Int64)) { writer =>
            writer.write(batch)
          }
        }
      }
      thrown.getMessage should include("an absent long column cannot be spilled through a stream")
    }
  }

  it should "refuse to spill an absent Int32 column" in {
    val column = Int32Column.withAbsence(
      Array(0),
      Validity.allValid(1),
      Validity.pack(1, _ => true),
      1,
    )
    val batch = new ColumnBatch(Vector(LogicalType.Int32), Vector(column), 1)
    withDir { dir =>
      val thrown = intercept[IllegalArgumentException] {
        SpillStore.withSpill(dir) { store =>
          val _ = store.write(Vector(batch))
        }
      }
      thrown.getMessage should include("an absent column cannot be spilled")
    }
  }

  private def present(values: Array[Long]): Int64Column =
    Int64Column.of(values, Validity.allValid(values.length), values.length)

  private def triple(key: Int64Column, left: Int64Column, right: Int64Column): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64, LogicalType.Int64),
      Vector(key, left, right),
      key.length,
    )

  private def int64(batch: ColumnBatch): Int64Column =
    batch.columns.headOption match
      case Some(column: Int64Column) => column
      case other => fail(s"expected Int64, got $other")

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def nullKey(row: Any): Boolean =
    row match
      case (null, _) => true
      case _ => false

  private def withDir(body: Path => Unit): Unit =
    val dir = Files.createTempDirectory("scarlet-long-outer")
    try body(dir)
    finally
      SpillStore.listForTest(dir).foreach(path => Files.deleteIfExists(path))
      val _ = Files.deleteIfExists(dir)
