package com.ewoodbury.scarlet.columnar

import java.nio.file.{Files, Path}
import java.nio.{ByteBuffer, ByteOrder}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class TestSpillStore extends AnyFunSuite with Matchers:

  private def corruptBatch(location: Path, batch: Int, count: Int): Unit =
    val bytes = Files.readAllBytes(location)
    val entry = SpillLayout.FooterEntryBytes
    val footerAt = bytes.length - 4 - count * entry
    val cursor = ByteBuffer
      .wrap(bytes, footerAt + batch * entry, entry)
      .order(ByteOrder.LITTLE_ENDIAN)
    val offset = cursor.getLong
    val length = cursor.getLong.toInt
    val at = offset.toInt + length / 2
    bytes(at) = (bytes(at) ^ 0xff).toByte
    Files.write(location, bytes)

  private def tempDir(): Path =
    Files.createTempDirectory("scarlet-spill-test")

  private def withDir(body: Path => Unit): Unit =
    val dir = tempDir()
    try body(dir)
    finally
      SpillStore.listForTest(dir).foreach(path => Files.deleteIfExists(path))
      Files.deleteIfExists(dir)

  private def intBatch(values: Seq[Int]): ColumnBatch =
    val column = Int32Column.of(values.toArray, Validity.allValid(values.length), values.length)
    new ColumnBatch(Vector(LogicalType.Int32), Vector(column), values.length)

  private def intPairBatch(rows: Seq[(Int, Int)]): ColumnBatch =
    val keys = Int32Column.of(rows.map(_._1).toArray, Validity.allValid(rows.length), rows.length)
    val values = Int64Column.of(rows.map(_._2.toLong).toArray, Validity.allValid(rows.length), rows.length)
    new ColumnBatch(Vector(LogicalType.Int32, LogicalType.Int64), Vector(keys, values), rows.length)

  private def nullableIntBatch(rows: Seq[Option[Int]]): ColumnBatch =
    val words = Validity.allocate(rows.length)
    val values = new Array[Int](rows.length)
    rows.zipWithIndex.foreach { (row, index) =>
      row match
        case Some(value) =>
          values(index) = value
          Validity.setBit(words, index)
        case None => ()
    }
    val column = Int32Column.of(values, Validity.fromWords(words, rows.length), rows.length)
    new ColumnBatch(Vector(LogicalType.Int32), Vector(column), rows.length)


  private def fingerprint(batch: ColumnBatch): (Vector[LogicalType], Int, Vector[Vector[(Long, Boolean)]]) =
    val columns = batch.columns.map {
      case column: Int32Column =>
        (0 until batch.length).map(row => (column.values(row).toLong, column.isValid(row))).toVector
      case column: Int64Column =>
        (0 until batch.length).map(row => (column.values(row), column.isValid(row))).toVector
      case other => fail(s"unexpected column ${other.logicalType}")
    }.toVector
    (batch.schema.toVector, batch.length, columns)

  private def assertSame(actual: Vector[ColumnBatch], expected: Vector[ColumnBatch]): Unit =
    actual.map(fingerprint) shouldBe expected.map(fingerprint)

  test("round-trips int columns with validity") {
    withDir { dir =>
      val batches = Vector(intBatch(Seq(1, 2, 3)), nullableIntBatch(Seq(Some(4), None, Some(6))))
      val store = SpillStore.open(dir)
      try
        val file = store.write(batches)
        assertSame(file.read(), batches)
      finally store.close()
    }
  }

  test("round-trips a two-column batch") {
    withDir { dir =>
      val batches = Vector(intPairBatch(Seq(1 -> 10, 2 -> 20)))
      val store = SpillStore.open(dir)
      try assertSame(store.write(batches).read(), batches)
      finally store.close()
    }
  }

  test("round-trips an empty batch and a zero-length batch in the middle") {
    withDir { dir =>
      val empty = intBatch(Seq.empty)
      val batches = Vector(intBatch(Seq(1)), empty, intBatch(Seq(2, 3)))
      val store = SpillStore.open(dir)
      try
        val read = store.write(batches).read()
        read.map(_.length) shouldBe Seq(1, 0, 2)
        assertSame(read, batches)
      finally store.close()
    }
  }

  test("batches yields one batch at a time and agrees with read") {
    withDir { dir =>
      val batches = Vector(
        intBatch(Seq(1, 2, 3)),
        nullableIntBatch(Seq(Some(4), None, Some(6))),
        intBatch(Seq.empty),
        intBatch(Seq(7)),
      )
      val store = SpillStore.open(dir)
      try
        val file = store.write(batches)
        val iterator = file.batches()
        try
          iterator.hasNext shouldBe true
          iterator.hasNext shouldBe true
          val streamed = Vector(iterator.next(), iterator.next(), iterator.next(), iterator.next())
          iterator.hasNext shouldBe false
          assertSame(streamed, batches)
          assertSame(file.read(), batches)
        finally iterator.close()
      finally store.close()
    }
  }

  test("batches throws on a bad checksum before returning that batch") {
    withDir { dir =>
      val batches = Vector(intBatch(Seq(1, 2, 3)), intBatch(Seq(4, 5, 6, 7, 8)))
      val store = SpillStore.open(dir)
      try
        val file = store.write(batches)
        corruptBatch(file.location, batch = 1, count = 2)
        val iterator = file.batches()
        try
          val first = iterator.next()
          assertSame(Vector(first), Vector(batches.head))
          an[IllegalStateException] should be thrownBy iterator.next()
        finally iterator.close()
      finally store.close()
    }
  }

  test("a corrupted payload fails the checksum") {
    withDir { dir =>
      val store = SpillStore.open(dir)
      try
        val file = store.write(Vector(intBatch(Seq(1, 2, 3))))
        val bytes = Files.readAllBytes(file.location)
        bytes(bytes.length / 2) = (bytes(bytes.length / 2) ^ 0xff).toByte
        Files.write(file.location, bytes)
        an[IllegalStateException] should be thrownBy file.read()
      finally store.close()
    }
  }

  test("a corrupted footer fails the checksum") {
    withDir { dir =>
      val store = SpillStore.open(dir)
      try
        val file = store.write(Vector(intBatch(Seq(1, 2, 3))))
        val bytes = Files.readAllBytes(file.location)
        bytes(bytes.length - 6) = (bytes(bytes.length - 6) ^ 0xff).toByte
        Files.write(file.location, bytes)
        an[IllegalStateException] should be thrownBy file.read()
      finally store.close()
    }
  }

  test("refuses a dictionary string column") {
    withDir { dir =>
      val dictionary = DictUtf8Column(Seq("a", "b"))
      val batch = new ColumnBatch(Vector(LogicalType.Utf8Dict), Vector(dictionary), 2)
      val store = SpillStore.open(dir)
      try
        an[IllegalArgumentException] should be thrownBy store.write(Vector(batch))
      finally store.close()
    }
  }

  test("deletes its files after success and after a failure") {
    withDir { dir =>
      val store = SpillStore.open(dir)
      store.write(Vector(intBatch(Seq(1))))
      store.close()
      SpillStore.listForTest(dir) shouldBe empty

      an[IllegalStateException] should be thrownBy
        SpillStore.withSpill(dir, maxFiles = 100, maxBytes = 8L) { spill =>
          spill.write(Vector(intBatch(Seq(1, 2, 3, 4))))
        }
      SpillStore.listForTest(dir) shouldBe empty
    }
  }

  test("enforces the file cap at the boundary") {
    withDir { dir =>
      val store = SpillStore.open(dir, maxFiles = 3, maxBytes = 1L << 20)
      try
        store.write(Vector(intBatch(Seq(1))))
        store.write(Vector(intBatch(Seq(2))))
        an[IllegalStateException] should be thrownBy store.write(Vector(intBatch(Seq(3))))
      finally store.close()
    }
  }

  test("enforces the byte cap at the boundary") {
    withDir { dir =>
      val batch = intBatch((0 until 100).toSeq)
      val store = SpillStore.open(dir, maxFiles = 100, maxBytes = 64L)
      try an[IllegalStateException] should be thrownBy store.write(Vector(batch))
      finally store.close()
      SpillStore.listForTest(dir) shouldBe empty
    }
  }

  test("the byte cap is exclusive at the exact file size") {
    withDir { dir =>
      val batch = intBatch((0 until 100).toSeq)
      val probe = SpillStore.open(dir, maxFiles = 100, maxBytes = 1L << 20)
      val size =
        try Files.size(probe.write(Vector(batch)).location)
        finally probe.close()
      SpillStore.listForTest(dir).foreach(path => Files.deleteIfExists(path))

      val fits = SpillStore.open(dir, maxFiles = 100, maxBytes = size + 1L)
      try fits.write(Vector(batch)) should not be null
      finally fits.close()

      val exact = SpillStore.open(dir, maxFiles = 100, maxBytes = size)
      try an[IllegalStateException] should be thrownBy exact.write(Vector(batch))
      finally exact.close()
    }
  }

  test("sweeps dead pid files and keeps live ones") {
    withDir { dir =>
      val live = dir.resolve(s"live.${ProcessHandle.current().pid}.spill")
      val dead = dir.resolve("dead.999999999.spill")
      val liveTemp = dir.resolve(s"busy.${ProcessHandle.current().pid}.tmp")
      val deadTemp = dir.resolve("stale.999999999.tmp")
      Seq(live, dead, liveTemp, deadTemp).foreach(path => Files.write(path, Array[Byte](1, 2, 3)))

      val store = SpillStore.open(dir)
      try
        Files.exists(live) shouldBe true
        Files.exists(liveTemp) shouldBe true
        Files.exists(dead) shouldBe false
        Files.exists(deadTemp) shouldBe false
      finally
        store.close()
        Seq(live, liveTemp).foreach(path => Files.deleteIfExists(path))
    }
  }

  test("a second close is a no-op") {
    withDir { dir =>
      val store = SpillStore.open(dir)
      store.write(Vector(intBatch(Seq(1))))
      store.close()
      store.close()
      SpillStore.listForTest(dir) shouldBe empty
    }
  }

  test("a closed store refuses a write") {
    withDir { dir =>
      val store = SpillStore.open(dir)
      store.close()
      an[IllegalStateException] should be thrownBy store.write(Vector(intBatch(Seq(1))))
    }
  }
