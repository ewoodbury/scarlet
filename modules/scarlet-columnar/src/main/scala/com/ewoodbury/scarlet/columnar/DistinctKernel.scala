package com.ewoodbury.scarlet.columnar

/**
 * Whole-row dedupe for a single-column int or string source.
 *
 * The input is scattered by value hash, so every duplicate of a value lands in one bucket for any
 * positive partition count. Each bucket is then deduped with an open-addressed set, which keeps
 * memory at `O(distinct values in the bucket)` and allocates nothing per row. A null is one
 * distinct value and survives exactly once; it is tracked by a flag rather than a sentinel, so a
 * real `0` or `""` cannot be confused with it. String buckets are concatenated before that set is
 * built, so a code compare is a string compare.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.ArrayEquals",
    "org.wartremover.warts.While",
    "org.wartremover.warts.Var",
  ),
)
object DistinctKernel:

  /** Scatter `batches` by value and dedupe each output partition. */
  def ints(batches: Vector[ColumnBatch], partitions: Int): Vector[ColumnBatch] =
    require(partitions > 0, s"partitions must be positive, got $partitions")
    val buckets = Array.fill(partitions)(Vector.empty[ColumnBatch])
    batches.foreach { batch =>
      val scattered = HashExchange.partitionInt32(batch, 0, partitions, carryNullKeys = true)
      var part = 0
      while (part < partitions) {
        buckets(part) = buckets(part) :+ scattered(part)
        part += 1
      }
    }
    Vector.tabulate(partitions) { part =>
      val parts = buckets(part).filter(_.length > 0)
      if (parts.isEmpty) then BatchCodec.ints(Seq.empty)
      else dedupe(ColumnBatches.concat(parts))
    }

  /**
   * Scatter `batches` by string hash, keeping nulls, and dedupe each output partition. An empty
   * input is `partitions` empty string batches.
   */
  def strings(batches: Vector[ColumnBatch], partitions: Int): Vector[ColumnBatch] =
    require(partitions > 0, s"partitions must be positive, got $partitions")
    if (batches.isEmpty) then Vector.fill(partitions)(BatchCodec.strings(Seq.empty))
    else
      HashExchange
        .partitionAllUtf8(
          batches,
          keyOrdinal = 0,
          numPartitions = partitions,
          parallelism = 1,
          carryNullKeys = true,
        )
        .map(dedupeStrings)

  private def dedupe(batch: ColumnBatch): ColumnBatch =
    val keys = ColumnHash.int32At(batch, 0)
    val values = keys.values
    val words = keys.validity.words
    val rows = batch.length
    val out = new Array[Int](rows)
    val outWords = new Array[Long]((rows + 63) >>> 6)
    val table = new IntSet(math.max(4, rows))
    var hasNull = false
    var written = 0
    var row = 0
    while (row < rows) do
      if (Validity.isSet(words, row)) then
        val value = values(row)
        if (table.add(value)) then
          out(written) = value
          Validity.setBit(outWords, written)
          written += 1
      else if (!hasNull) then
        hasNull = true
        out(written) = 0
        written += 1
      row += 1
    val trimmed = out.slice(0, written)
    val trimmedWords =
      if (written == 0) then Validity.allocate(0) else outWords.slice(0, (written + 63) >>> 6)
    val validity = Validity.fromWords(trimmedWords, written)
    new ColumnBatch(
      batch.schema,
      Vector(Int32Column.of(trimmed, validity, written)),
      written,
    )

  /**
   * One bucket, one dictionary: [[HashExchange.partitionAllUtf8]] concatenates the bucket before
   * this runs, so a code compare is a string compare. A null is a flag, not a code.
   */
  private def dedupeStrings(batch: ColumnBatch): ColumnBatch =
    val keys = ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, 0), 0)
    val codes = keys.codes
    val words = keys.validity.words
    val rows = batch.length
    val out = new Array[Int](rows)
    val outWords = new Array[Long]((rows + 63) >>> 6)
    val table = new IntSet(math.max(4, rows))
    var hasNull = false
    var written = 0
    var row = 0
    while (row < rows) do
      if (Validity.isSet(words, row)) then
        val code = codes(row)
        if (table.add(code)) then
          out(written) = code
          Validity.setBit(outWords, written)
          written += 1
      else if (!hasNull) then
        hasNull = true
        out(written) = 0
        written += 1
      row += 1
    val trimmed = out.slice(0, written)
    val trimmedWords =
      if (written == 0) then Validity.allocate(0) else outWords.slice(0, (written + 63) >>> 6)
    val validity = Validity.fromWords(trimmedWords, written)
    new ColumnBatch(
      batch.schema,
      Vector(DictUtf8Column.of(keys.dictionary, trimmed, validity, written)),
      written,
    )

  /** Open-addressed int set. A parallel `used` word keeps every int value legal, including 0. */
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
