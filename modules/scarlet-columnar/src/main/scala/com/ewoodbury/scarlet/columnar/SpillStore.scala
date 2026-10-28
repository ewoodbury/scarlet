package com.ewoodbury.scarlet.columnar

import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.CRC32
import java.util.{Arrays, UUID}

import scala.annotation.tailrec

/**
 * Writes column batches to a file and reads them back.
 *
 * A spill file is a header, a payload, a footer, and a CRC32 of the payload and the footer. The
 * header holds the magic `SCRSPILL`, the version, the schema, the batch count, and the footer
 * offset and length. Each footer record holds the batch's file offset, byte length, and row count.
 * Int32 and Int64 columns are stored with their validity words. A zero-row batch is legal. Every
 * other column type is refused, including a dictionary string column.
 *
 * The bytes are written to `<id>.<pid>.tmp`, fsynced, then moved into `<id>.<pid>.spill` with an
 * atomic move. `maxFiles` and `maxBytes` are exclusive upper bounds on the files this store has
 * published. A write that would reach either cap throws `IllegalStateException`, and the message
 * names `maxFiles` or `maxBytes`. The default directory is
 * `System.getProperty("java.io.tmpdir")/scarlet-spill`, which is outside the repository. The
 * constructor takes the directory explicitly so tests can pass their own.
 *
 * [[sweep]] removes every `*.tmp` file and every spill file whose pid fails `pidIsAlive`. The
 * constructor runs that sweep before the first write, so opening a store drops temp files and
 * files left by a dead process.
 *
 * [[SpillStore.withSpill]] and [[close]] delete the files this store published. Both run on
 * success, on failure, and when the body is cancelled.
 *
 * A reader currently loads the whole file into memory. That is fine for the sizes the first
 * consumer spills, and a streaming reader is a follow-up rather than an assumption.
 */
final class SpillStore private (
    val directory: Path,
    val maxFiles: Int,
    val maxBytes: Long,
) extends AutoCloseable:

  private val published = new AtomicReference(Vector.empty[SpillStore.Stored])
  private val closed = new java.util.concurrent.atomic.AtomicBoolean(false)

  locally {
    require(maxFiles > 0, s"maxFiles must be > 0, got $maxFiles")
    require(maxBytes > 0L, s"maxBytes must be > 0, got $maxBytes")
    val _ = Files.createDirectories(directory)
    sweep(SpillStore.processAlive)
  }

  def write(batches: Vector[ColumnBatch]): SpillFile = synchronized {
    if (closed.get()) then throw new IllegalStateException("spill store is closed")
    val schema = SpillStore.preflight(batches)
    val records = published.get
    if (records.length + 1 >= maxFiles) then
      throw new IllegalStateException(s"spill maxFiles cap is $maxFiles")
    val pid = ProcessHandle.current().pid
    val id = UUID.randomUUID.toString
    val temporary = directory.resolve(s"$id.$pid.tmp")
    val destination = directory.resolve(s"$id.$pid.spill")
    try SpillCodec.write(temporary, schema, batches)
    catch
      case thrown: Throwable =>
        val _ = Files.deleteIfExists(temporary)
        throw thrown
    val size = Files.size(temporary)
    val used = records.foldLeft(0L)((total, record) => total + record.bytes)
    if (used >= maxBytes || size >= maxBytes - used) then
      val _ = Files.deleteIfExists(temporary)
      throw new IllegalStateException(s"spill maxBytes cap is $maxBytes")
    try
      Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE)
      SpillStore.syncDirectory(directory)
    catch
      case thrown: Throwable =>
        val _ = Files.deleteIfExists(temporary)
        throw thrown
    try
      val _ = published.updateAndGet(current => current :+ SpillStore.Stored(destination, size))
    catch
      case thrown: Throwable =>
        val _ = Files.deleteIfExists(destination)
        throw thrown
    new SpillFile(destination, forget)
  }

  /** Drops one published file and frees its share of the caps. */
  private def forget(location: Path): Unit =
    // Untrack only after the file is gone. A failed delete keeps the record so `close` can retry
    // and the caps still count the bytes.
    val _ = Files.deleteIfExists(location)
    val _ = published.updateAndGet(current => current.filterNot(_.path == location))

  /**
   * Deletes temp files and spill files whose process is gone. The constructor calls this with the
   * operating system's liveness check before any write.
   */
  def sweep(pidIsAlive: Long => Boolean): Unit = synchronized {
    val removed = SpillStore.listFiles(directory).filter { path =>
      Files.isRegularFile(path) && SpillStore.condemned(path.getFileName.toString, pidIsAlive)
    }
    // A temp file belongs to the process in its name; a live writer keeps it.
    removed.foreach { path =>
      try
        val _ = Files.deleteIfExists(path)
      catch case _: Throwable => ()
    }
    val gone = removed.toSet
    val _ =
      published.updateAndGet(current => current.filterNot(record => gone.contains(record.path)))
  }

  /** Deletes every file this store has published. A second call is a no-op. */
  def close(): Unit =
    val records = synchronized {
      if (closed.getAndSet(true)) then Vector.empty else published.getAndSet(Vector.empty)
    }
    records.foreach { record =>
      try
        val _ = Files.deleteIfExists(record.path)
      catch case _: Throwable => ()
    }

object SpillStore:

  /** Exclusive cap. A store may publish at most `DefaultMaxFiles - 1` files. */
  val DefaultMaxFiles: Int = 1024

  /** Exclusive cap, 8 GiB. A store may publish at most one byte less than this. */
  val DefaultMaxBytes: Long = 8L * 1024L * 1024L * 1024L

  /** `java.io.tmpdir/scarlet-spill`. Never a directory inside the repository. */
  def defaultDirectory: Path =
    Path.of(System.getProperty("java.io.tmpdir"), "scarlet-spill")

  def open(directory: Path, maxFiles: Int, maxBytes: Long): SpillStore =
    new SpillStore(directory, maxFiles, maxBytes)

  def open(directory: Path): SpillStore =
    open(directory, DefaultMaxFiles, DefaultMaxBytes)

  def open(): SpillStore = open(defaultDirectory)

  /**
   * Opens a store on `dir` and deletes its files when `body` returns, throws, or is cancelled.
   * Caps are the defaults.
   */
  def withSpill[A](dir: Path)(body: SpillStore => A): A =
    withSpill(dir, DefaultMaxFiles, DefaultMaxBytes)(body)

  /** Same cleanup as [[withSpill]], with explicit caps. */
  def withSpill[A](dir: Path, maxFiles: Int, maxBytes: Long)(body: SpillStore => A): A =
    val store = open(dir, maxFiles, maxBytes)
    try body(store)
    finally store.close()

  private[columnar] final case class Stored(path: Path, bytes: Long)

  private val FinalName = """(.+)\.(\d+)\.spill""".r

  private def processAlive(pid: Long): Boolean =
    val handle = ProcessHandle.of(pid)
    handle.isPresent && handle.get.isAlive

  private def preflight(batches: Vector[ColumnBatch]): Vector[LogicalType] =
    val schema = batches.headOption match
      case Some(batch) => batch.schema.toVector
      case None => throw new IllegalArgumentException("spill needs at least one batch")
    batches.foreach { batch =>
      require(batch.schema == schema, "spill batches must share a schema")
      batch.columns.foreach(supported)
    }
    schema

  private def supported(column: Column): Unit = column match
    case _: Int32Column => ()
    case _: Int64Column => ()
    case other =>
      throw new IllegalArgumentException(s"${other.logicalType} columns cannot be spilled")

  private def condemned(name: String, pidIsAlive: Long => Boolean): Boolean =
    val trimmed = if (name.endsWith(".tmp")) then name.dropRight(4) else name
    val digits =
      if (name.endsWith(".tmp")) then
        trimmed.lastIndexOf('.') match
          case at if at >= 0 => Some(trimmed.substring(at + 1))
          case _ => None
      else
        name match
          case FinalName(_, found) => Some(found)
          case _ => None
    digits.flatMap(parsePid) match
      case Some(pid) => !pidIsAlive(pid)
      case None =>
        // A temp file without a pid cannot be attributed; leave it for a human.
        false

  private def parsePid(digits: String): Option[Long] =
    if (digits.length > 19) then None
    else
      try Some(java.lang.Long.parseLong(digits))
      catch case _: NumberFormatException => None

  /**
   * Forces the directory entry after a rename, so a crash cannot lose the file's name. Not every
   * platform allows opening a directory for read, so a failure here is not fatal.
   */
  private[columnar] def syncDirectory(directory: Path): Unit =
    try
      val channel = FileChannel.open(directory, StandardOpenOption.READ)
      try channel.force(true)
      finally channel.close()
    catch case _: java.io.IOException => ()

  /** Lists the files in a spill directory. Tests use this to assert cleanup. */
  private[scarlet] def listForTest(directory: Path): Vector[Path] = listFiles(directory)

  private def listFiles(directory: Path): Vector[Path] =
    if (!Files.isDirectory(directory)) then Vector.empty
    else
      val stream = Files.newDirectoryStream(directory)
      try collect(stream.iterator(), Vector.empty)
      finally stream.close()

  @tailrec
  private def collect(iterator: java.util.Iterator[Path], acc: Vector[Path]): Vector[Path] =
    if (iterator.hasNext) then collect(iterator, acc :+ iterator.next())
    else acc

/** One published spill file. [[read]] checks the CRC32 before it returns batches. */
final class SpillFile private[columnar] (val location: Path, release: Path => Unit):

  def read(): Vector[ColumnBatch] = SpillCodec.read(location)

  def delete(): Unit = release(location)

/** Little-endian layout shared by the writer, the reader, and the tests that corrupt a file. */
private[columnar] object SpillLayout:

  val MagicText: String = "SCRSPILL"
  val Version: Int = 1
  val Int32Tag: Int = 1
  val Int64Tag: Int = 2
  val FooterEntryBytes: Int = 20

  def magic: Array[Byte] = MagicText.getBytes(StandardCharsets.US_ASCII)

  def headerSize(columnCount: Int): Int = 32 + columnCount * 4

  /** File offset of the footer-offset field. */
  def footerField(columnCount: Int): Int = 20 + columnCount * 4

  def tag(logicalType: LogicalType): Int = logicalType match
    case LogicalType.Int32 => Int32Tag
    case LogicalType.Int64 => Int64Tag
    case other => throw new IllegalArgumentException(s"$other columns cannot be spilled")

  def logicalType(tag: Int): LogicalType = tag match
    case Int32Tag => LogicalType.Int32
    case Int64Tag => LogicalType.Int64
    case other => throw new IllegalArgumentException(s"unknown spill column tag $other")

private object SpillCodec:

  private final case class BatchSpan(offset: Long, length: Long, rows: Int)

  def write(temporary: Path, schema: Vector[LogicalType], batches: Vector[ColumnBatch]): Unit =
    val channel = FileChannel.open(
      temporary,
      StandardOpenOption.CREATE_NEW,
      StandardOpenOption.WRITE,
    )
    try
      val header = headerBytes(schema, batches.length, 0L, 0)
      val crc = new CRC32
      val _ = writeFully(channel, crc, header, checksum = false)
      val spans = writePayload(channel, crc, batches, header.length.toLong)
      val footer = footerBytes(spans)
      val _ = writeFully(channel, crc, footer, checksum = true)
      val _ = writeFully(channel, crc, intBytes(crc.getValue.toInt), checksum = false)
      patchHeader(
        channel,
        schema.length,
        header.length.toLong + payloadLength(spans),
        footer.length,
      )
      channel.force(true)
    finally channel.close()

  def read(location: Path): Vector[ColumnBatch] =
    require(Files.isRegularFile(location), s"spill file is missing: $location")
    val bytes = Files.readAllBytes(location)
    val header = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    require(bytes.length >= SpillLayout.headerSize(0), "truncated spill file")
    val magic = new Array[Byte](SpillLayout.MagicText.length)
    header.get(magic)
    require(Arrays.equals(magic, SpillLayout.magic), "spill magic mismatch")
    val version = header.getInt
    require(version == SpillLayout.Version, s"spill version $version is not supported")
    val columnCount = header.getInt
    require(columnCount >= 0, s"column count must be >= 0, got $columnCount")
    require(columnCount <= (Int.MaxValue - 32) / 4, "spill schema is too wide")
    require(bytes.length >= SpillLayout.headerSize(columnCount), "truncated spill file")
    val schema =
      (0 until columnCount).iterator.map(_ => SpillLayout.logicalType(header.getInt)).toVector
    val batchCount = header.getInt
    require(batchCount >= 0, s"batch count must be >= 0, got $batchCount")
    require(
      batchCount <= Int.MaxValue / SpillLayout.FooterEntryBytes,
      "spill batch count is too large",
    )
    val footerOffset = header.getLong
    val footerLength = header.getInt
    require(footerLength >= 0, s"spill footer length must be >= 0, got $footerLength")
    require(
      footerLength == batchCount * SpillLayout.FooterEntryBytes,
      "spill footer length does not match the batch count",
    )
    val headerSize = SpillLayout.headerSize(columnCount)
    require(footerOffset >= headerSize.toLong, "spill footer starts inside the header")
    val crcAt = footerOffset + footerLength.toLong
    require(crcAt >= footerOffset, "spill footer range overflows")
    require(crcAt + 4L == bytes.length.toLong, "spill file length does not match the footer")
    verifyCrc(bytes, headerSize, footerOffset, footerLength)
    val spans = readFooter(bytes, footerOffset, batchCount)
    decode(bytes, schema, spans, footerOffset)

  private def writePayload(
      channel: FileChannel,
      crc: CRC32,
      batches: Vector[ColumnBatch],
      origin: Long,
  ): Vector[BatchSpan] =
    val (_, spans) = batches.foldLeft((origin, Vector.empty[BatchSpan])) { (state, batch) =>
      val (cursor, acc) = state
      val written = writeBatch(channel, crc, batch)
      (cursor + written, acc :+ BatchSpan(cursor, written, batch.length))
    }
    spans

  private def writeBatch(channel: FileChannel, crc: CRC32, batch: ColumnBatch): Long =
    batch.columns.foldLeft(0L) { (written, column) =>
      written + writeColumn(channel, crc, column)
    }

  private def writeColumn(channel: FileChannel, crc: CRC32, column: Column): Long = column match
    case int32: Int32Column =>
      writeValidity(channel, crc, int32.validity, int32.length) +
        writeInts(channel, crc, int32.values, int32.length)
    case int64: Int64Column =>
      writeValidity(channel, crc, int64.validity, int64.length) +
        writeLongs(channel, crc, int64.values, int64.length)
    case other =>
      throw new IllegalArgumentException(s"${other.logicalType} columns cannot be spilled")

  private def writeValidity(
      channel: FileChannel,
      crc: CRC32,
      validity: Validity,
      length: Int,
  ): Long =
    val count = (length + 63) >>> 6
    val words = Arrays.copyOf(validity.words, count)
    val tail = length & 63
    if (tail != 0 && count > 0) then words(count - 1) = words(count - 1) & ((1L << tail) - 1L)
    val buffer = ByteBuffer.allocate(4 + count * 8).order(ByteOrder.LITTLE_ENDIAN)
    buffer.putInt(count)
    words.foreach(word => buffer.putLong(word))
    writeFully(channel, crc, buffer.array(), checksum = true)

  private def writeInts(
      channel: FileChannel,
      crc: CRC32,
      values: Array[Int],
      length: Int,
  ): Long =
    val buffer = ByteBuffer.allocate(length * 4).order(ByteOrder.LITTLE_ENDIAN)
    (0 until length).foreach(row => buffer.putInt(values(row)))
    writeFully(channel, crc, buffer.array(), checksum = true)

  private def writeLongs(
      channel: FileChannel,
      crc: CRC32,
      values: Array[Long],
      length: Int,
  ): Long =
    val buffer = ByteBuffer.allocate(length * 8).order(ByteOrder.LITTLE_ENDIAN)
    (0 until length).foreach(row => buffer.putLong(values(row)))
    writeFully(channel, crc, buffer.array(), checksum = true)

  private def payloadLength(spans: Vector[BatchSpan]): Long =
    spans.foldLeft(0L)((total, span) => total + span.length)

  private def headerBytes(
      schema: Vector[LogicalType],
      batchCount: Int,
      footerOffset: Long,
      footerLength: Int,
  ): Array[Byte] =
    val buffer =
      ByteBuffer.allocate(SpillLayout.headerSize(schema.length)).order(ByteOrder.LITTLE_ENDIAN)
    buffer.put(SpillLayout.magic)
    buffer.putInt(SpillLayout.Version)
    buffer.putInt(schema.length)
    schema.foreach(logicalType => buffer.putInt(SpillLayout.tag(logicalType)))
    buffer.putInt(batchCount)
    buffer.putLong(footerOffset)
    buffer.putInt(footerLength)
    buffer.array()

  private def footerBytes(spans: Vector[BatchSpan]): Array[Byte] =
    val buffer = ByteBuffer
      .allocate(spans.length * SpillLayout.FooterEntryBytes)
      .order(ByteOrder.LITTLE_ENDIAN)
    spans.foreach { span =>
      buffer.putLong(span.offset)
      buffer.putLong(span.length)
      buffer.putInt(span.rows)
    }
    buffer.array()

  private def patchHeader(
      channel: FileChannel,
      columnCount: Int,
      footerOffset: Long,
      footerLength: Int,
  ): Unit =
    val buffer = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
    buffer.putLong(footerOffset)
    buffer.putInt(footerLength)
    buffer.flip()
    writeAt(channel, buffer, SpillLayout.footerField(columnCount).toLong)

  private def intBytes(value: Int): Array[Byte] =
    val buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
    buffer.putInt(value)
    buffer.array()

  // FileChannel.write may return after a short write, so the loop drains the buffer.
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def writeFully(
      channel: FileChannel,
      crc: CRC32,
      bytes: Array[Byte],
      checksum: Boolean,
  ): Long =
    if (checksum) then crc.update(bytes)
    val buffer = ByteBuffer.wrap(bytes)
    var pending = buffer.hasRemaining
    while (pending) {
      channel.write(buffer)
      pending = buffer.hasRemaining
    }
    bytes.length.toLong

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def writeAt(channel: FileChannel, buffer: ByteBuffer, position: Long): Unit =
    var cursor = position
    var pending = buffer.hasRemaining
    while (pending) {
      val wrote = channel.write(buffer, cursor)
      cursor += wrote
      pending = buffer.hasRemaining
    }

  private def verifyCrc(
      bytes: Array[Byte],
      headerSize: Int,
      footerOffset: Long,
      footerLength: Int,
  ): Unit =
    val region = footerOffset - headerSize + footerLength.toLong
    require(region >= 0L && region <= Int.MaxValue.toLong, "spill checksum range is invalid")
    val crc = new CRC32
    crc.update(bytes, headerSize, region.toInt)
    val stored = ByteBuffer
      .wrap(bytes, (footerOffset + footerLength).toInt, 4)
      .order(ByteOrder.LITTLE_ENDIAN)
      .getInt
    if (crc.getValue.toInt != stored) then throw new IllegalStateException("spill CRC32 mismatch")

  private def readFooter(
      bytes: Array[Byte],
      footerOffset: Long,
      batchCount: Int,
  ): Vector[BatchSpan] =
    val footerBytes = SpillLayout.FooterEntryBytes
    val slice = Arrays.copyOfRange(
      bytes,
      footerOffset.toInt,
      footerOffset.toInt + batchCount * footerBytes,
    )
    val cursor = new Cursor(slice)
    (0 until batchCount).iterator.map { _ =>
      BatchSpan(cursor.getLong, cursor.getLong, cursor.getInt)
    }.toVector

  private def decode(
      bytes: Array[Byte],
      schema: Vector[LogicalType],
      spans: Vector[BatchSpan],
      footerOffset: Long,
  ): Vector[ColumnBatch] =
    val headerSize = SpillLayout.headerSize(schema.length)
    val packed = spans.foldLeft(headerSize.toLong) { (cursor, span) =>
      require(span.offset == cursor, "spill batches are not packed")
      require(span.length >= 0L, "spill batch length must be >= 0")
      require(span.rows >= 0, "spill row count must be >= 0")
      val next = cursor + span.length
      require(next <= footerOffset, "spill batch runs into the footer")
      next
    }
    require(packed == footerOffset, "spill payload length does not match the footer")
    spans.map { span =>
      require(span.length <= Int.MaxValue.toLong, "spill batch is too large")
      val slice = Arrays.copyOfRange(
        bytes,
        span.offset.toInt,
        span.offset.toInt + span.length.toInt,
      )
      val cursor = new Cursor(slice)
      val columns = schema.map(logicalType => readColumn(cursor, logicalType, span.rows))
      require(cursor.remaining == 0, "spill batch length does not match its columns")
      new ColumnBatch(schema, columns, span.rows)
    }

  private def readColumn(cursor: Cursor, logicalType: LogicalType, rows: Int): Column =
    val validity = readValidity(cursor, rows)
    logicalType match
      case LogicalType.Int32 => Int32Column.of(readInts(cursor, rows), validity, rows)
      case LogicalType.Int64 => Int64Column.of(readLongs(cursor, rows), validity, rows)
      case other => throw new IllegalArgumentException(s"$other columns cannot be spilled")

  private def readValidity(cursor: Cursor, rows: Int): Validity =
    val count = cursor.getInt
    val expected = (rows + 63) >>> 6
    require(count == expected, s"validity words $count do not match row count $rows")
    val words = new Array[Long](count)
    (0 until count).foreach(index => words(index) = cursor.getLong)
    Validity.fromWords(words, rows)

  private def readInts(cursor: Cursor, rows: Int): Array[Int] =
    val values = new Array[Int](rows)
    (0 until rows).foreach(row => values(row) = cursor.getInt)
    values

  private def readLongs(cursor: Cursor, rows: Int): Array[Long] =
    val values = new Array[Long](rows)
    (0 until rows).foreach(row => values(row) = cursor.getLong)
    values

  private final class Cursor(bytes: Array[Byte]):
    private val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

    def remaining: Int = buffer.remaining

    def getInt: Int =
      require(buffer.remaining >= 4, "truncated spill file")
      buffer.getInt

    def getLong: Long =
      require(buffer.remaining >= 8, "truncated spill file")
      buffer.getLong

    def getBytes(count: Int): Array[Byte] =
      require(buffer.remaining >= count, "truncated spill file")
      val out = new Array[Byte](count)
      buffer.get(out)
      out
