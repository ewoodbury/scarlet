package com.ewoodbury.scarlet.columnar

import java.util.{Arrays, HashMap}

/**
 * Inner shuffle-hash join of dictionary string keys and `Int32` values.
 *
 * [[inner]] partitions both sides with [[HashExchange.partitionAllUtf8]], builds the left bucket,
 * and probes the right. The two sides keep independent dictionaries, so the table is not keyed by
 * dictionary code. Each distinct build string is assigned an id once. A probe row looks that
 * string up once. Duplicate build keys emit the full cartesian product. Within a probe row the
 * build chain is newest-first. That walk is a kernel detail, not a public contract.
 *
 * A null key is dropped by the exchange before the table, and a clear key bit is not inserted and
 * not probed. `""` is a present key. Every live value cell must be present. Spare capacity past
 * `length` is ignored, which is what a filter publishes. The planner refuses a null value, so a
 * clear live value bit is a bug and throws `IllegalStateException`. There is no spill and no build
 * budget. Output columns are `(key, left, right)`. The key is the probe row's dictionary and code.
 * One batch per partition. An empty side, or a partition with no matches, publishes
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

  def inner(
      buildBatches: Vector[ColumnBatch],
      probeBatches: Vector[ColumnBatch],
      partitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(partitions > 0, s"partitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (buildBatches.isEmpty || probeBatches.isEmpty) then
      Vector.fill(partitions)(ColumnBatches.emptyStringJoin)
    else
      val buildBuckets =
        HashExchange.partitionAllUtf8(buildBatches, keyOrdinal = 0, partitions, parallelism)
      val probeBuckets =
        HashExchange.partitionAllUtf8(probeBatches, keyOrdinal = 0, partitions, parallelism)
      MorselScheduler.map(probeBuckets, parallelism) { (part, probeBatch) =>
        val build = buildBuckets.lift(part).getOrElse {
          throw new IllegalStateException(s"missing build partition $part")
        }
        joinPartition(build, probeBatch)
      }

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
      requireValues(buildValues, "build")
      requireValues(probeValues, "probe")
      new BuildTable(buildKeys, buildValues).write(probeKeys, probeValues)

  private def utf8At(batch: ColumnBatch, ordinal: Int): DictUtf8Column =
    ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private def requireValues(values: Int32Column, side: String): Unit =
    if (!Validity.livePresent(values.validity, values.length)) then
      throw new IllegalStateException(s"$side value column is not fully present")

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
      val buildValueValues = buildValues.values
      val probeValueValues = probeValues.values
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
              outCodes(filled) = probeCodes(row)
              outBuild(filled) = buildValueValues(buildRow)
              outProbe(filled) = probeValueValues(row)
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
      capacity: Int,
      rows: Int,
  ): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32, LogicalType.Int32),
      Vector(
        DictUtf8Column.of(dictionary, codes, Validity.prefixValid(capacity, rows), rows),
        Int32Column.of(buildValues, Validity.prefixValid(capacity, rows), rows),
        Int32Column.of(probeValues, Validity.prefixValid(capacity, rows), rows),
      ),
      rows,
    )

  private def doubled(capacity: Int): Int =
    if (capacity >= ColumnHash.MaxCapacity) then
      throw new IllegalStateException(s"join output reached $capacity rows")
    capacity << 1
