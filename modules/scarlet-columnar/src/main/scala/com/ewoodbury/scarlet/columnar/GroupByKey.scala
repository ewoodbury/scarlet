package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Groups pair batches by key.
 *
 * [[int32]] groups `(Int, Int)`. [[HashExchange.partitionAll]] drops a null key and buckets with
 * `floorMod(key, partitions)`, the same rule `HashPartitioner` uses because `Int.hashCode` is the
 * value. [[strings]] groups `(String, Int)`. [[HashExchange.partitionAllUtf8]] drops a null key
 * and buckets with `floorMod(StringHash.hash(text), partitions)`. `""` is a present key. Each
 * bucket is one dictionary: unlike dictionaries are remapped when the bucket is concatenated, so
 * code equality is string equality.
 *
 * Each bucket keeps first-seen key order and input value order. A null value is not dropped. A
 * clear value bit is a planner bug and throws `IllegalArgumentException`, including a null value
 * on a row whose key is also null. That check runs before the exchange, which would otherwise drop
 * the null key and hide the value. [[int32Selected]] and [[stringsSelected]] check only
 * `rows(0 until count)`, so a null value on a dropped row does not throw. [[stringsSelected]]
 * scatters with [[HashExchange.partitionAllSelectedUtf8]], which drops a null key, keeps `""`, and
 * shares the source dictionary. One grouped batch is published per partition, including an empty
 * bucket. There is no partial combine, no spill, and no sort.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object GroupByKey:

  def int32(
      batches: Vector[ColumnBatch],
      partitions: Int,
      parallelism: Int,
  ): Vector[GroupedIntBatch] =
    require(partitions > 0, s"partitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (batches.isEmpty) then Vector.fill(partitions)(GroupedIntBatch.empty)
    else
      require(
        batches.forall(batch =>
          batch.schema.length == 2 && batch.schema(0) == LogicalType.Int32 &&
            batch.schema(1) == LogicalType.Int32,
        ),
        "groupByKey expects (Int32, Int32) batches",
      )
      batches.foreach(requirePresentValues)
      val buckets = HashExchange.partitionAll(batches, keyOrdinal = 0, partitions, parallelism)
      MorselScheduler.map(buckets, parallelism)((_, bucket) => group(bucket))

  /**
   * [[int32]] for a selection. Present values are checked on `rows(0 until count)` only. The
   * exchange scatters those rows, then each bucket uses the dense [[group]].
   */
  def int32Selected(
      selected: Vector[Selected],
      partitions: Int,
      parallelism: Int,
  ): Vector[GroupedIntBatch] =
    require(partitions > 0, s"partitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (selected.isEmpty) then Vector.fill(partitions)(GroupedIntBatch.empty)
    else
      require(
        selected.forall(one => isIntPair(one.batch)),
        "groupByKey expects (Int32, Int32) batches",
      )
      selected.foreach(requirePresentSelection)
      val buckets =
        HashExchange.partitionAllSelected(selected, keyOrdinal = 0, partitions, parallelism)
      MorselScheduler.map(buckets, parallelism)((_, bucket) => group(bucket))

  def strings(
      batches: Vector[ColumnBatch],
      partitions: Int,
      parallelism: Int,
  ): Vector[GroupedStringBatch] =
    require(partitions > 0, s"partitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (batches.isEmpty) then Vector.fill(partitions)(GroupedStringBatch.empty)
    else
      require(
        batches.forall(batch =>
          batch.schema.length == 2 && batch.schema(0) == LogicalType.Utf8Dict &&
            batch.schema(1) == LogicalType.Int32,
        ),
        "groupByKey expects (Utf8Dict, Int32) batches",
      )
      batches.foreach(requirePresentValues)
      val buckets =
        HashExchange.partitionAllUtf8(batches, keyOrdinal = 0, partitions, parallelism)
      MorselScheduler.map(buckets, parallelism)((_, bucket) => groupStrings(bucket))

  /**
   * [[strings]] for a selection. Present values are checked on `rows(0 until count)` only. The
   * exchange scatters those rows, then each bucket uses the dense [[groupStrings]].
   */
  def stringsSelected(
      selected: Vector[Selected],
      partitions: Int,
      parallelism: Int,
  ): Vector[GroupedStringBatch] =
    require(partitions > 0, s"partitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (selected.isEmpty) then Vector.fill(partitions)(GroupedStringBatch.empty)
    else
      require(
        selected.forall(one => isStringPair(one.batch)),
        "groupByKey expects (Utf8Dict, Int32) batches",
      )
      selected.foreach(requirePresentSelection)
      val buckets =
        HashExchange.partitionAllSelectedUtf8(selected, keyOrdinal = 0, partitions, parallelism)
      MorselScheduler.map(buckets, parallelism)((_, bucket) => groupStrings(bucket))

  private def requirePresentValues(batch: ColumnBatch): Unit =
    val values = ColumnHash.int32At(batch, 1)
    require(
      Validity.livePresent(values.validity, values.length),
      nullValueMessage,
    )

  /** A null value on a dropped row is ignored. A null value on a selected row still throws. */
  private def requirePresentSelection(selected: Selected): Unit =
    val values = ColumnHash.int32At(selected.batch, 1)
    val words = values.validity.words
    val rows = selected.rows
    var slot = 0
    while (slot < selected.count) {
      require(Validity.isSet(words, rows(slot)), nullValueMessage)
      slot += 1
    }

  private def isIntPair(batch: ColumnBatch): Boolean =
    batch.schema.length == 2 && batch.schema(0) == LogicalType.Int32 &&
      batch.schema(1) == LogicalType.Int32

  private def isStringPair(batch: ColumnBatch): Boolean =
    batch.schema.length == 2 && batch.schema(0) == LogicalType.Utf8Dict &&
      batch.schema(1) == LogicalType.Int32

  private val nullValueMessage =
    "groupByKey value column has a null; a clear value bit is a planner bug"

  private def group(batch: ColumnBatch): GroupedIntBatch =
    val keys = ColumnHash.int32At(batch, 0)
    val values = ColumnHash.int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    requirePresentValues(batch)
    if (keys.length == 0) then GroupedIntBatch.empty
    else fill(keys, values, ColumnHash.fullyPresent(keys))

  private def fill(
      keys: Int32Column,
      values: Int32Column,
      presentKeys: Boolean,
  ): GroupedIntBatch =
    val rows = keys.length
    val capacity = ColumnHash.tableSize(rows)
    require(capacity >= 2 * rows, s"group table capacity $capacity is below 2 * $rows")
    val table = new GroupTable(capacity)
    val keyValues = keys.values
    val valueValues = values.values
    val keyWords = keys.validity.words
    var row = 0
    while (row < rows) {
      if (presentKeys || Validity.isSet(keyWords, row)) then table.add(keyValues(row))
      row += 1
    }
    val groups = table.groups
    if (groups == 0) then GroupedIntBatch.empty
    else
      val offsets = new Array[Int](groups + 1)
      var cursor = 0
      var group = 0
      while (group < groups) {
        offsets(group) = cursor
        cursor += table.count(group)
        group += 1
      }
      offsets(groups) = cursor
      val outValues = new Array[Int](cursor)
      val writeAt = Arrays.copyOf(offsets, groups)
      row = 0
      while (row < rows) {
        if (presentKeys || Validity.isSet(keyWords, row)) then
          val atGroup = table.indexOf(keyValues(row))
          val dest = writeAt(atGroup)
          outValues(dest) = valueValues(row)
          writeAt(atGroup) = dest + 1
        row += 1
      }
      val outKeys = new Array[Int](groups)
      group = 0
      while (group < groups) {
        outKeys(group) = table.key(group)
        group += 1
      }
      GroupedIntBatch(
        Int32Column.of(outKeys, Validity.allValid(groups), groups),
        offsets,
        outValues,
      )

  private def groupStrings(batch: ColumnBatch): GroupedStringBatch =
    val keys = utf8At(batch, 0)
    val values = ColumnHash.int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    requirePresentValues(batch)
    if (keys.length == 0) then GroupedStringBatch.empty
    else fillStrings(keys, values, ColumnHash.fullyPresent(keys))

  private def fillStrings(
      keys: DictUtf8Column,
      values: Int32Column,
      presentKeys: Boolean,
  ): GroupedStringBatch =
    val rows = keys.length
    val capacity = ColumnHash.tableSize(rows)
    require(capacity >= 2 * rows, s"group table capacity $capacity is below 2 * $rows")
    val table = new GroupTable(capacity)
    val codes = keys.codes
    val valueValues = values.values
    val keyWords = keys.validity.words
    var row = 0
    while (row < rows) {
      if (presentKeys || Validity.isSet(keyWords, row)) then table.add(codes(row))
      row += 1
    }
    val groups = table.groups
    if (groups == 0) then GroupedStringBatch.empty
    else
      val offsets = new Array[Int](groups + 1)
      var cursor = 0
      var group = 0
      while (group < groups) {
        offsets(group) = cursor
        cursor += table.count(group)
        group += 1
      }
      offsets(groups) = cursor
      val outValues = new Array[Int](cursor)
      val writeAt = Arrays.copyOf(offsets, groups)
      row = 0
      while (row < rows) {
        if (presentKeys || Validity.isSet(keyWords, row)) then
          val atGroup = table.indexOf(codes(row))
          val dest = writeAt(atGroup)
          outValues(dest) = valueValues(row)
          writeAt(atGroup) = dest + 1
        row += 1
      }
      val outCodes = new Array[Int](groups)
      group = 0
      while (group < groups) {
        outCodes(group) = table.key(group)
        group += 1
      }
      GroupedStringBatch(
        DictUtf8Column.of(keys.dictionary, outCodes, Validity.allValid(groups), groups),
        offsets,
        outValues,
      )

  private def utf8At(batch: ColumnBatch, ordinal: Int): DictUtf8Column =
    ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private final class GroupTable(capacity: Int):
    private val mask: Int = capacity - 1
    private val limit: Int = capacity >>> 1
    private val keys: Array[Int] = new Array[Int](capacity)
    private val state: Array[Byte] = new Array[Byte](capacity)
    private val slots: Array[Int] = new Array[Int](capacity)
    private val groupOfSlot: Array[Int] = new Array[Int](capacity)
    private val counts: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0

    def groups: Int = occupied

    def count(group: Int): Int = counts(group)

    def key(group: Int): Int = keys(slots(group))

    def indexOf(key: Int): Int = groupOfSlot(locate(key))

    def add(key: Int): Unit =
      val slot = locate(key)
      if (state(slot) == 0) then
        if (occupied >= limit) then
          throw new IllegalStateException(s"groupByKey table is full at $occupied groups")
        state(slot) = 1
        keys(slot) = key
        groupOfSlot(slot) = occupied
        slots(occupied) = slot
        occupied += 1
      counts(groupOfSlot(slot)) += 1

    private def locate(key: Int): Int =
      var slot = ColumnHash.mix(key) & mask
      while (state(slot) != 0 && keys(slot) != key) {
        slot = (slot + 1) & mask
      }
      slot
