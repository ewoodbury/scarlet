package com.ewoodbury.scarlet.columnar

import java.util.{Arrays, HashMap}

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Inner shuffle-hash join of dictionary string keys and `Int32` values.
 *
 * [[inner]] partitions both sides, builds the left bucket, and probes the right. A
 * [[StringJoinInput.Dense]] side uses [[HashExchange.partitionAllUtf8]]. A
 * [[StringJoinInput.Selected]] side uses [[HashExchange.partitionAllSelectedUtf8]] and is not
 * sliced: the exchange scatters `rows(0 until count)`, and the build table and the probe read
 * those rows. The batch overload wraps both sides as dense. The two sides keep independent
 * dictionaries, so the table is not keyed by dictionary code. Each distinct build string is
 * assigned an id once. A probe row looks that string up once. Duplicate build keys emit the full
 * cartesian product. Within a probe row the build chain is newest-first. That walk is a kernel
 * detail, not a public contract.
 *
 * A null key is dropped by the exchange before the table, and a clear key bit is not inserted and
 * not probed. `""` is a present key. A clear value bit is copied. A clear validity bit is a null
 * value, not an absence bit, and the matched row is kept. Spare capacity past `length` is ignored,
 * which is what a filter publishes. A null value outside a selection is not live. Spill is off
 * unless [[ScarletConf.spillEnabled]] is set. A build bucket whose
 * [[StringHashJoinSpill.retainedBytes]] estimate is above [[ScarletConf.spillBudgetBytes]] is
 * finished by [[StringHashJoinSpill]]. That estimate includes the dictionary. A bucket at or under
 * the budget, and every bucket while spill is off, is joined here and writes nothing. Output
 * columns are `(key, left, right)`. The key is the probe row's dictionary and code. One batch per
 * partition. An empty side, or a partition with no matches, publishes
 * [[ColumnBatches.emptyStringJoin]].
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Var",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.MutableDataStructures",
  ),
)
object StringHashJoin:

  /**
   * The row-selection class. [[StringJoinInput.Selected]] is also named Selected, so the case
   * parameter uses this alias and does not become a vector of the case itself.
   */
  type Survivor = Selected

  /**
   * One side of a string inner shuffle-hash join.
   *
   * [[StringJoinInput.Dense]] is read row by row. [[StringJoinInput.Selected]] names the live
   * prefix. The exchange scatters that prefix and does not slice it.
   */
  enum StringJoinInput:
    case Dense(batches: Vector[ColumnBatch])
    case Selected(parts: Vector[Survivor])

  /** Dense batches. Both sides take the [[StringJoinInput]] overload with no slice. */
  def inner(
      buildBatches: Vector[ColumnBatch],
      probeBatches: Vector[ColumnBatch],
      partitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    inner(
      StringJoinInput.Dense(buildBatches),
      StringJoinInput.Dense(probeBatches),
      partitions,
      parallelism,
    )

  /**
   * Inner shuffle-hash join. `build` is the left input and `probe` is the right.
   * [[StringJoinInput.Dense]] is partitioned with [[HashExchange.partitionAllUtf8]].
   * [[StringJoinInput.Selected]] is partitioned with [[HashExchange.partitionAllSelectedUtf8]] and
   * is not sliced. A null key is dropped. `""` is a present key. A clear value bit is copied and
   * the matched row is kept. It is not an absence bit. Output columns are `(key, left, right)`.
   *
   * Spill is off unless [[ScarletConf.spillEnabled]] is set. A build bucket whose
   * [[StringHashJoinSpill.retainedBytes]] estimate is above [[ScarletConf.spillBudgetBytes]] is
   * finished by [[StringHashJoinSpill]]. A run whose buckets all fit stays on [[MorselScheduler]]
   * and writes nothing. A selected side is scattered before that choice, so a spill sees survivor
   * rows only.
   */
  def inner(
      build: StringJoinInput,
      probe: StringJoinInput,
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"partitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (hasNoBatches(build) || hasNoBatches(probe)) then
      Vector.fill(numPartitions)(ColumnBatches.emptyStringJoin)
    else
      val buildBuckets = exchange(build, numPartitions, parallelism)
      val probeBuckets = exchange(probe, numPartitions, parallelism)
      val conf = ScarletConf.get
      if (buildBuckets.exists(bucket => StringHashJoinSpill.exceeds(bucket, conf))) then
        StringHashJoinSpill.finish(buildBuckets, probeBuckets, parallelism, conf)
      else joinExchanged(buildBuckets, probeBuckets, parallelism)

  /** Already-exchanged buckets. No second partition, and no spill store. */
  private def joinExchanged(
      buildBuckets: Vector[ColumnBatch],
      probeBuckets: Vector[ColumnBatch],
      parallelism: Int,
  ): Vector[ColumnBatch] =
    MorselScheduler.map(probeBuckets, parallelism) { (part, probeBatch) =>
      val built = buildBuckets.lift(part).getOrElse {
        throw new IllegalStateException(s"missing build partition $part")
      }
      joinPartition(built, probeBatch)
    }

  /**
   * One already-exchanged bucket pair. Does not partition and does not spill.
   * [[StringHashJoinSpill]] probes with this. A null key is not inserted and not probed. `""` is a
   * present key. A clear value bit is copied with the matched row. It is not an absence bit.
   */
  private[scarlet] def joinBucket(build: ColumnBatch, probe: ColumnBatch): ColumnBatch =
    joinPartition(build, probe)

  private def exchange(
      input: StringJoinInput,
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    input match
      case StringJoinInput.Dense(batches) =>
        HashExchange.partitionAllUtf8(batches, keyOrdinal = 0, numPartitions, parallelism)
      case StringJoinInput.Selected(parts) =>
        HashExchange.partitionAllSelectedUtf8(parts, keyOrdinal = 0, numPartitions, parallelism)

  private def hasNoBatches(input: StringJoinInput): Boolean = input match
    case StringJoinInput.Dense(batches) => batches.isEmpty
    case StringJoinInput.Selected(parts) => parts.isEmpty

  private def joinPartition(build: ColumnBatch, probe: ColumnBatch): ColumnBatch =
    if (build.length == 0 || probe.length == 0) then ColumnBatches.emptyStringJoin
    else
      val buildKeys = utf8At(build, 0)
      val buildValues = ColumnHash.int32At(build, 1)
      val probeKeys = utf8At(probe, 0)
      val probeValues = ColumnHash.int32At(probe, 1)
      require(
        buildKeys.length == buildValues.length,
        "build key and value columns differ in length",
      )
      require(
        probeKeys.length == probeValues.length,
        "probe key and value columns differ in length",
      )
      new BuildTable(buildKeys, buildValues).write(probeKeys, probeValues)

  private def utf8At(batch: ColumnBatch, ordinal: Int): DictUtf8Column =
    ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private final class BuildTable(buildKeys: DictUtf8Column, buildValues: Int32Column):
    private val rows: Int = buildKeys.length
    private val head: Array[Int] = new Array[Int](rows)
    private val next: Array[Int] = new Array[Int](rows)

    /** One id per distinct build string. The map grows; a huge row count must not pre-size it. */
    private val ids = new HashMap[String, Integer](math.max(1, math.min(rows, 1024)))
    private var nextId: Int = 0

    Arrays.fill(head, -1)
    Arrays.fill(next, -1)

    def write(probeKeys: DictUtf8Column, probeValues: Int32Column): ColumnBatch =
      insert()
      val probeRows = probeKeys.length
      var outCapacity = math.max(16, probeRows)
      var filled = 0
      var outCodes = new Array[Int](outCapacity)
      var outBuild = new Array[Int](outCapacity)
      var outProbe = new Array[Int](outCapacity)
      var buildWords = Validity.allocate(outCapacity)
      var probeWords = Validity.allocate(outCapacity)
      val buildValueValues = buildValues.values
      val buildValueWords = buildValues.validity.words
      val probeValueValues = probeValues.values
      val probeValueWords = probeValues.validity.words
      val probeCodes = probeKeys.codes
      val probeDictionary = probeKeys.dictionary
      val probeKeyWords = probeKeys.validity.words
      var row = 0
      while (row < probeRows) {
        if (Validity.isSet(probeKeyWords, row)) then
          val id = lookup(probeDictionary(probeCodes(row)))
          if (id >= 0) then
            var buildRow = head(id)
            while (buildRow != -1) {
              if (filled == outCapacity) then
                outCapacity = doubled(outCapacity)
                outCodes = Arrays.copyOf(outCodes, outCapacity)
                outBuild = Arrays.copyOf(outBuild, outCapacity)
                outProbe = Arrays.copyOf(outProbe, outCapacity)
                buildWords = growWords(buildWords, outCapacity)
                probeWords = growWords(probeWords, outCapacity)
              outCodes(filled) = probeCodes(row)
              if (Validity.isSet(buildValueWords, buildRow)) then
                outBuild(filled) = buildValueValues(buildRow)
                Validity.setBit(buildWords, filled)
              if (Validity.isSet(probeValueWords, row)) then
                outProbe(filled) = probeValueValues(row)
                Validity.setBit(probeWords, filled)
              filled += 1
              buildRow = next(buildRow)
            }
        row += 1
      }
      if (filled == 0) then ColumnBatches.emptyStringJoin
      else
        publish(
          probeKeys.dictionary,
          outCodes,
          outBuild,
          outProbe,
          buildWords,
          probeWords,
          outCapacity,
          filled,
        )

    private def insert(): Unit =
      val codes = buildKeys.codes
      val dictionary = buildKeys.dictionary
      val keyWords = buildKeys.validity.words
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then
          val id = idOf(dictionary(codes(row)))
          next(row) = head(id)
          head(id) = row
        row += 1
      }

    /** One id per distinct build string. A later row with the same string reuses that id. */
    private def idOf(text: String): Int =
      // `HashMap.get` returns a boxed `Integer`; `eq null` tests absence without unboxing.
      val existing = ids.get(text)
      if (existing eq null) then
        val assigned = nextId
        nextId += 1
        ids.put(text, Integer.valueOf(assigned))
        assigned
      else existing.intValue()

    /** `-1` when the probe string was never built. One map lookup, and no insert. */
    private def lookup(text: String): Int =
      val existing = ids.get(text)
      if (existing eq null) then -1 else existing.intValue()

  private def publish(
      dictionary: Array[String],
      codes: Array[Int],
      buildValues: Array[Int],
      probeValues: Array[Int],
      buildWords: Array[Long],
      probeWords: Array[Long],
      capacity: Int,
      rows: Int,
  ): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32, LogicalType.Int32),
      Vector(
        DictUtf8Column.of(dictionary, codes, Validity.prefixValid(capacity, rows), rows),
        Int32Column.of(buildValues, Validity.fromWords(buildWords, capacity), rows),
        Int32Column.of(probeValues, Validity.fromWords(probeWords, capacity), rows),
      ),
      rows,
    )

  private def doubled(capacity: Int): Int =
    if (capacity >= ColumnHash.MaxCapacity) then
      throw new IllegalStateException(s"join output reached $capacity rows")
    capacity << 1

  private def growWords(words: Array[Long], rowCapacity: Int): Array[Long] =
    val needed = (rowCapacity + 63) >>> 6
    if (words.length == needed) words else Arrays.copyOf(words, needed)
