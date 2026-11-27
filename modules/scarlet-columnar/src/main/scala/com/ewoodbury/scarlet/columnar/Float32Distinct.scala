package com.ewoodbury.scarlet.columnar

/**
 * Distinct of one `Float32` column.
 *
 * Scatter uses the canonical bits, then each partition keeps one copy. `-0.0f` shares `0.0f`'s
 * bucket and set slot. Every `NaN` is kept. One null is one clear bit.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object Float32Distinct:

  def apply(batches: Vector[ColumnBatch], partitions: Int): Vector[ColumnBatch] =
    require(partitions > 0, s"partitions must be positive, got $partitions")
    val buckets = Array.fill(partitions)(Vector.empty[ColumnBatch])
    batches.foreach { batch =>
      val scattered = scatter(batch, partitions)
      var part = 0
      while (part < partitions) do
        buckets(part) = buckets(part) :+ scattered(part)
        part += 1
    }
    Vector.tabulate(partitions) { part =>
      val parts = buckets(part).filter(_.length > 0)
      if (parts.isEmpty) then Float32Codec.empty
      else dedupe(ColumnBatches.concat(parts))
    }

  private def dedupe(batch: ColumnBatch): ColumnBatch =
    val keys = float32(batch)
    val values = keys.values
    val words = keys.validity.words
    val rows = batch.length
    val out = new Array[Float](rows)
    val outWords = new Array[Long]((rows + 63) >>> 6)
    val table = new IntSet(math.max(4, rows))
    var hasNull = false
    var written = 0
    var row = 0
    while (row < rows) do
      if (Validity.isSet(words, row)) then
        val value = values(row)
        val keep =
          if (java.lang.Float.isNaN(value)) then true
          else table.add(canonicalBits(value))
        if (keep) then
          out(written) = value
          Validity.setBit(outWords, written)
          written += 1
      else if (!hasNull) then
        hasNull = true
        out(written) = 0.0f
        written += 1
      row += 1
    publish(batch, out, outWords, written)

  /** `-0.0f` bits are `Integer.MinValue`. Scala `==` treats that value as `0.0f`. */
  private def canonicalBits(value: Float): Int =
    val bits = java.lang.Float.floatToIntBits(value)
    if (bits == Integer.MIN_VALUE) then 0 else bits

  private def scatter(batch: ColumnBatch, partitions: Int): Vector[ColumnBatch] =
    val keys = float32(batch)
    val rows = batch.length
    if (partitions == 1) then Vector(batch)
    else
      val dest = new Array[Int](rows)
      val counts = new Array[Int](partitions)
      val values = keys.values
      val words = keys.validity.words
      var row = 0
      while (row < rows) do
        val part =
          if (Validity.isSet(words, row)) then
            Math.floorMod(Integer.hashCode(canonicalBits(values(row))), partitions)
          else 0
        dest(row) = part
        counts(part) += 1
        row += 1
      val outputs = counts.map(count => new Array[Float](count))
      val outWords = counts.map(count => Validity.allocate(count))
      val cursor = new Array[Int](partitions)
      row = 0
      while (row < rows) do
        val part = dest(row)
        val at = cursor(part)
        outputs(part)(at) = values(row)
        if (Validity.isSet(words, row)) then Validity.setBit(outWords(part), at)
        cursor(part) = at + 1
        row += 1
      Vector.tabulate(partitions) { part =>
        val column = Float32Column.of(
          outputs(part),
          Validity.fromWords(outWords(part), counts(part)),
          counts(part),
        )
        new ColumnBatch(batch.schema, Vector(column), counts(part))
      }

  private def publish(
      batch: ColumnBatch,
      out: Array[Float],
      outWords: Array[Long],
      written: Int,
  ): ColumnBatch =
    val trimmed = out.slice(0, written)
    val trimmedWords =
      if (written == 0) then Validity.allocate(0) else outWords.slice(0, (written + 63) >>> 6)
    val validity = Validity.fromWords(trimmedWords, written)
    new ColumnBatch(batch.schema, Vector(Float32Column.of(trimmed, validity, written)), written)

  private def float32(batch: ColumnBatch): Float32Column =
    ColumnKernel.columnAt(batch, 0) match
      case column: Float32Column => column
      case other =>
        throw new IllegalArgumentException(
          s"column 0 is ${other.logicalType}, expected Float32",
        )

  /** Open-addressed int set. A parallel `used` word keeps every int legal, including 0. */
  private final class IntSet(rows: Int):
    private var keys = new Array[Int](nextPowerOfTwo(math.max(4, rows)))
    private var used = new Array[Boolean](keys.length)
    private var size = 0

    def add(value: Int): Boolean =
      if (size * 2 >= keys.length) then grow()
      var slot = mix(value) & (keys.length - 1)
      while (used(slot)) do
        if (keys(slot) == value) then return false
        slot = (slot + 1) & (keys.length - 1)
      used(slot) = true
      keys(slot) = value
      size += 1
      true

    private def grow(): Unit =
      val oldKeys = keys
      val oldUsed = used
      keys = new Array[Int](oldKeys.length * 2)
      used = new Array[Boolean](keys.length)
      var index = 0
      while (index < oldKeys.length) do
        if (oldUsed(index)) then
          var slot = mix(oldKeys(index)) & (keys.length - 1)
          while (used(slot)) do slot = (slot + 1) & (keys.length - 1)
          used(slot) = true
          keys(slot) = oldKeys(index)
        index += 1

    private def mix(value: Int): Int =
      val hash = value * 0x9e3779b1
      hash ^ (hash >>> 16)

  private def nextPowerOfTwo(value: Int): Int =
    var size = 1
    while (size < value) do size <<= 1
    size
