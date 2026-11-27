package com.ewoodbury.scarlet.columnar

import java.util.Arrays
import java.util.concurrent.atomic.AtomicLong

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Groups pair batches by key.
 *
 * [[int32]] groups `(Int, Int)`. [[HashExchange.partitionAll]] drops a null key and buckets with
 * `floorMod(key, partitions)`, the same rule `HashPartitioner` uses because `Int.hashCode` is the
 * value. [[int64]] groups `(Long, Long)`. [[HashExchange.partitionAllInt64]] drops a null key and
 * buckets with `floorMod(Long.hashCode(key), partitions)`. The table stores the full `Long` and
 * probes with [[ColumnHash.mix]] of that hash code. Equality is `Long` equality. There is no
 * selection variant. [[strings]] groups `(String, Int)`. [[HashExchange.partitionAllUtf8]] drops a
 * null key and buckets with `floorMod(StringHash.hash(text), partitions)`. `""` is a present key.
 * Each bucket is one dictionary: unlike dictionaries are remapped when the bucket is concatenated,
 * so code equality is string equality.
 *
 * Each bucket keeps first-seen key order and input value order. A null key is dropped by the
 * exchange, and a null value on that key goes with it. A null value on a present key stays in the
 * group. [[int32Selected]] and [[stringsSelected]] keep a null on a selected present key and
 * ignore a null on a dropped row. An over-budget post-exchange bucket that still has a clear value
 * bit throws `IllegalArgumentException` before a spill file is opened. [[stringsSelected]]
 * scatters with [[HashExchange.partitionAllSelectedUtf8]], which drops a null key, keeps `""`, and
 * shares the source dictionary. One grouped batch is published per partition, including an empty
 * bucket. There is no partial combine and no sort.
 *
 * Spill is off unless [[ScarletConf.spillEnabled]] is set. While it is off, an over-budget bucket
 * stays in memory and writes nothing. While it is on, a post-exchange bucket whose table estimate
 * is over [[ScarletConf.spillBudgetBytes]] is sliced in input order and grouped one morsel at a
 * time. A morsel that still does not fit throws [[ClosedKernelRefusal]] before its table is
 * allocated. The appended groups are not bounded by the budget. The morsel width is
 * [[ScarletConf.columnarBatchSize]]. Under-budget buckets, and every bucket while spill is off,
 * keep [[MorselScheduler.map]].
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object GroupByKey:

  private val builtValueLists = new AtomicLong

  /** Groups whose value arrays were allocated since [[resetValueListsBuilt]]. */
  private[scarlet] def valueListsBuilt: Long = builtValueLists.get()

  /** Clears [[valueListsBuilt]]. */
  private[scarlet] def resetValueListsBuilt(): Unit = builtValueLists.set(0L)

  /** Counts one materialised group. A zero-group batch adds nothing. */
  private[scarlet] def noteValueLists(groups: Int): Unit =
    if (groups > 0) then
      builtValueLists.addAndGet(groups.toLong)
      ()

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
      val buckets = HashExchange.partitionAll(batches, keyOrdinal = 0, partitions, parallelism)
      finishBuckets(buckets, parallelism, GroupByKeySpill.exceeds, GroupByKeySpill.finish, group)

  /**
   * [[int32]] for a selection. The exchange scatters `rows(0 until count)`, drops a null key, and
   * keeps a null value on a present key. Each bucket then uses the dense [[group]].
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
      val buckets =
        HashExchange.partitionAllSelected(selected, keyOrdinal = 0, partitions, parallelism)
      finishBuckets(buckets, parallelism, GroupByKeySpill.exceeds, GroupByKeySpill.finish, group)

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
      val buckets =
        HashExchange.partitionAllUtf8(batches, keyOrdinal = 0, partitions, parallelism)
      finishBuckets(
        buckets,
        parallelism,
        StringGroupByKeySpill.exceeds,
        StringGroupByKeySpill.finish,
        groupStrings,
      )

  /**
   * Groups `(Long, Long)`. An empty input publishes `partitions` empty batches. Every batch must
   * be `[Int64, Int64]`. [[HashExchange.partitionAllInt64]] drops a null key. A null value on a
   * present key stays in the group. Each bucket is one [[LongGroupTable]].
   */
  def int64(
      batches: Vector[ColumnBatch],
      partitions: Int,
      parallelism: Int,
  ): Vector[GroupedLongBatch] =
    require(partitions > 0, s"partitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (batches.isEmpty) then Vector.fill(partitions)(GroupedLongBatch.empty)
    else
      require(
        batches.forall(batch =>
          batch.schema.length == 2 && batch.schema(0) == LogicalType.Int64 &&
            batch.schema(1) == LogicalType.Int64,
        ),
        "groupByKey expects (Int64, Int64) batches",
      )
      val buckets =
        HashExchange.partitionAllInt64(batches, keyOrdinal = 0, partitions, parallelism)
      finishBuckets(
        buckets,
        parallelism,
        LongGroupByKeySpill.exceeds,
        LongGroupByKeySpill.finish,
        groupLongs,
      )

  /**
   * Groups `(Int, Long)` after the value has already been widened.
   *
   * Every batch is `[Int32, Int64]`. A null key is dropped by [[HashExchange.partitionAll]]. A
   * null value is copied with its validity bit and stays in the group. Spill stays off for this
   * shape: an over-budget bucket is still grouped in memory, and [[GroupByKeySpill.finish]] is not
   * used, because that path groups int values.
   */
  def intLong(
      batches: Vector[ColumnBatch],
      partitions: Int,
      parallelism: Int,
  ): Vector[GroupedIntLongBatch] =
    require(partitions > 0, s"partitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (batches.isEmpty) then Vector.fill(partitions)(GroupedIntLongBatch.empty)
    else
      require(
        batches.forall(batch =>
          batch.schema.length == 2 && batch.schema(0) == LogicalType.Int32 &&
            batch.schema(1) == LogicalType.Int64,
        ),
        "groupByKey expects (Int32, Int64) batches",
      )
      val buckets = HashExchange.partitionAll(batches, keyOrdinal = 0, partitions, parallelism)
      MorselScheduler.map(buckets, parallelism)((_, bucket) => groupIntLong(bucket))

  /**
   * [[strings]] for a selection. The exchange scatters `rows(0 until count)`, drops a null key,
   * and keeps a null value on a present key. Each bucket then uses the dense [[groupStrings]].
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
      val buckets =
        HashExchange.partitionAllSelectedUtf8(selected, keyOrdinal = 0, partitions, parallelism)
      finishBuckets(
        buckets,
        parallelism,
        StringGroupByKeySpill.exceeds,
        StringGroupByKeySpill.finish,
        groupStrings,
      )

  /**
   * Spill refusal. The bucket is already past the exchange, so a clear value bit belongs to a
   * present key. The caller throws this before opening a spill file.
   */
  private[columnar] def refuseSpilledValue(values: Column): Unit =
    require(Validity.livePresent(values.validity, values.length), nullValueMessage)

  private def isIntPair(batch: ColumnBatch): Boolean =
    batch.schema.length == 2 && batch.schema(0) == LogicalType.Int32 &&
      batch.schema(1) == LogicalType.Int32

  private def isStringPair(batch: ColumnBatch): Boolean =
    batch.schema.length == 2 && batch.schema(0) == LogicalType.Utf8Dict &&
      batch.schema(1) == LogicalType.Int32

  private val nullValueMessage =
    "groupByKey value column has a null; a clear value bit is a planner bug"

  private def finishBuckets[A](
      buckets: Vector[ColumnBatch],
      parallelism: Int,
      exceeds: (ColumnBatch, ScarletConf) => Boolean,
      finish: (ColumnBatch, ScarletConf) => A,
      groupOne: ColumnBatch => A,
  ): Vector[A] =
    val conf = ScarletConf.get
    if (buckets.exists(bucket => exceeds(bucket, conf))) then
      buckets.map(bucket => finish(bucket, conf))
    else MorselScheduler.map(buckets, parallelism)((_, bucket) => groupOne(bucket))

  private[columnar] def group(batch: ColumnBatch): GroupedIntBatch =
    val keys = ColumnHash.int32At(batch, 0)
    val values = ColumnHash.int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
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
      noteValueLists(groups)
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
      val presentValues = ColumnHash.fullyPresent(values)
      val valueWords = values.validity.words
      val outValueWords = if (presentValues) Array.emptyLongArray else Validity.allocate(cursor)
      val writeAt = Arrays.copyOf(offsets, groups)
      row = 0
      while (row < rows) {
        if (presentKeys || Validity.isSet(keyWords, row)) then
          val atGroup = table.indexOf(keyValues(row))
          val dest = writeAt(atGroup)
          outValues(dest) = valueValues(row)
          if (!presentValues && Validity.isSet(valueWords, row)) then
            Validity.setBit(outValueWords, dest)
          writeAt(atGroup) = dest + 1
        row += 1
      }
      val outKeys = new Array[Int](groups)
      group = 0
      while (group < groups) {
        outKeys(group) = table.key(group)
        group += 1
      }
      val valueBits =
        if (presentValues) then Validity.allValid(cursor)
        else Validity.fromWords(outValueWords, cursor)
      GroupedIntBatch(
        Int32Column.of(outKeys, Validity.allValid(groups), groups),
        offsets,
        outValues,
        valueBits,
      )

  private[columnar] def groupStrings(batch: ColumnBatch): GroupedStringBatch =
    val keys = utf8At(batch, 0)
    val values = ColumnHash.int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
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
      noteValueLists(groups)
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
      val presentValues = ColumnHash.fullyPresent(values)
      val valueWords = values.validity.words
      val outValueWords = if (presentValues) Array.emptyLongArray else Validity.allocate(cursor)
      val writeAt = Arrays.copyOf(offsets, groups)
      row = 0
      while (row < rows) {
        if (presentKeys || Validity.isSet(keyWords, row)) then
          val atGroup = table.indexOf(codes(row))
          val dest = writeAt(atGroup)
          outValues(dest) = valueValues(row)
          if (!presentValues && Validity.isSet(valueWords, row)) then
            Validity.setBit(outValueWords, dest)
          writeAt(atGroup) = dest + 1
        row += 1
      }
      val outCodes = new Array[Int](groups)
      group = 0
      while (group < groups) {
        outCodes(group) = table.key(group)
        group += 1
      }
      val valueBits =
        if (presentValues) then Validity.allValid(cursor)
        else Validity.fromWords(outValueWords, cursor)
      GroupedStringBatch(
        DictUtf8Column.of(keys.dictionary, outCodes, Validity.allValid(groups), groups),
        offsets,
        outValues,
        valueBits,
      )

  private[columnar] def groupLongs(batch: ColumnBatch): GroupedLongBatch =
    val keys = ColumnHash.int64At(batch, 0)
    val values = ColumnHash.int64At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    if (keys.length == 0) then GroupedLongBatch.empty
    else fillLongs(keys, values, ColumnHash.fullyPresent(keys))

  private def fillLongs(
      keys: Int64Column,
      values: Int64Column,
      presentKeys: Boolean,
  ): GroupedLongBatch =
    val rows = keys.length
    val capacity = ColumnHash.tableSize(rows)
    require(capacity >= 2 * rows, s"group table capacity $capacity is below 2 * $rows")
    val table = new LongGroupTable(capacity)
    val keyValues = keys.values
    val valueValues = values.values
    val keyWords = keys.validity.words
    var row = 0
    while (row < rows) {
      if (presentKeys || Validity.isSet(keyWords, row)) then table.add(keyValues(row))
      row += 1
    }
    val groups = table.groups
    if (groups == 0) then GroupedLongBatch.empty
    else
      noteValueLists(groups)
      val offsets = new Array[Int](groups + 1)
      var cursor = 0
      var group = 0
      while (group < groups) {
        offsets(group) = cursor
        cursor += table.count(group)
        group += 1
      }
      offsets(groups) = cursor
      val outValues = new Array[Long](cursor)
      val presentValues = ColumnHash.fullyPresent(values)
      val valueWords = values.validity.words
      val outValueWords = if (presentValues) Array.emptyLongArray else Validity.allocate(cursor)
      val writeAt = Arrays.copyOf(offsets, groups)
      row = 0
      while (row < rows) {
        if (presentKeys || Validity.isSet(keyWords, row)) then
          val atGroup = table.indexOf(keyValues(row))
          val dest = writeAt(atGroup)
          outValues(dest) = valueValues(row)
          if (!presentValues && Validity.isSet(valueWords, row)) then
            Validity.setBit(outValueWords, dest)
          writeAt(atGroup) = dest + 1
        row += 1
      }
      val outKeys = new Array[Long](groups)
      group = 0
      while (group < groups) {
        outKeys(group) = table.key(group)
        group += 1
      }
      val valueBits =
        if (presentValues) then Validity.allValid(cursor)
        else Validity.fromWords(outValueWords, cursor)
      GroupedLongBatch(
        Int64Column.of(outKeys, Validity.allValid(groups), groups),
        offsets,
        outValues,
        valueBits,
      )

  private[columnar] def groupIntLong(batch: ColumnBatch): GroupedIntLongBatch =
    val keys = ColumnHash.int32At(batch, 0)
    val values = ColumnHash.int64At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    if (keys.length == 0) then GroupedIntLongBatch.empty
    else fillIntLong(keys, values, ColumnHash.fullyPresent(keys))

  private def fillIntLong(
      keys: Int32Column,
      values: Int64Column,
      presentKeys: Boolean,
  ): GroupedIntLongBatch =
    val rows = keys.length
    val capacity = ColumnHash.tableSize(rows)
    require(capacity >= 2 * rows, s"group table capacity $capacity is below 2 * $rows")
    val table = new GroupTable(capacity)
    val keyValues = keys.values
    val valueValues = values.values
    val keyWords = keys.validity.words
    val valueWordsIn = values.validity.words
    val presentValues = ColumnHash.fullyPresent(values)
    var row = 0
    while (row < rows) {
      if (presentKeys || Validity.isSet(keyWords, row)) then table.add(keyValues(row))
      row += 1
    }
    val groups = table.groups
    if (groups == 0) then GroupedIntLongBatch.empty
    else
      noteValueLists(groups)
      val offsets = new Array[Int](groups + 1)
      var cursor = 0
      var group = 0
      while (group < groups) {
        offsets(group) = cursor
        cursor += table.count(group)
        group += 1
      }
      offsets(groups) = cursor
      val outValues = new Array[Long](cursor)
      val outValueWords = Validity.allocate(cursor)
      val writeAt = Arrays.copyOf(offsets, groups)
      row = 0
      while (row < rows) {
        if (presentKeys || Validity.isSet(keyWords, row)) then
          val atGroup = table.indexOf(keyValues(row))
          val dest = writeAt(atGroup)
          outValues(dest) = valueValues(row)
          if (presentValues || Validity.isSet(valueWordsIn, row)) then
            Validity.setBit(outValueWords, dest)
          writeAt(atGroup) = dest + 1
        row += 1
      }
      val outKeys = new Array[Int](groups)
      group = 0
      while (group < groups) {
        outKeys(group) = table.key(group)
        group += 1
      }
      GroupedIntLongBatch(
        Int32Column.of(outKeys, Validity.allValid(groups), groups),
        offsets,
        Int64Column.of(outValues, Validity.fromWords(outValueWords, cursor), cursor),
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

  /**
   * Open-addressed groups for a `Long` key. The stored key and the equality test are the full
   * `Long`. The probe mixes `Long.hashCode`, which is not unique, so `1L << 32` and `1L` share a
   * hash and stay distinct only because the key itself is compared.
   */
  private final class LongGroupTable(capacity: Int):
    private val mask: Int = capacity - 1
    private val limit: Int = capacity >>> 1
    private val keys: Array[Long] = new Array[Long](capacity)
    private val state: Array[Byte] = new Array[Byte](capacity)
    private val slots: Array[Int] = new Array[Int](capacity)
    private val groupOfSlot: Array[Int] = new Array[Int](capacity)
    private val counts: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0

    def groups: Int = occupied

    def count(group: Int): Int = counts(group)

    def key(group: Int): Long = keys(slots(group))

    def indexOf(key: Long): Int = groupOfSlot(locate(key))

    def add(key: Long): Unit =
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

    private def locate(key: Long): Int =
      var slot = ColumnHash.mix(java.lang.Long.hashCode(key)) & mask
      while (state(slot) != 0 && keys(slot) != key) {
        slot = (slot + 1) & mask
      }
      slot
