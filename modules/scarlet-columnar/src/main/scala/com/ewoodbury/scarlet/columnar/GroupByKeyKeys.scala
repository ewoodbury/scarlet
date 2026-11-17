package com.ewoodbury.scarlet.columnar

import java.util.Arrays

import scala.collection.immutable.ArraySeq

/**
 * Distinct keys of a `groupByKey` shuffle.
 *
 * Int keys use the same raw [[HashExchange.partitionAll]] as [[GroupByKey.int32]]. Long keys use
 * [[HashExchange.partitionAllInt64]]. String keys use [[HashExchange.partitionAllUtf8]]. A null
 * key is dropped. `""` is a present string key. `"é"` and `"e\u0301"` stay distinct. The bucket is
 * the same `floorMod` hash as that `groupByKey`. Each bucket then emits keys in the same order as
 * the row path's `groupMap(...).toSeq` on that key type, which follows the immutable map built
 * from `mutable.Map`, not insertion order and not a sort. No value array is allocated. This is not
 * [[HashCount]] and not [[DistinctKernel]]: the count kernel partial-combines, and distinct keeps
 * a null. [[GroupByKey.valueListsBuilt]] stays unchanged.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object GroupByKeyKeys:

  def int32(
      batches: Vector[ColumnBatch],
      partitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(partitions > 0, s"partitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (batches.isEmpty) then empty(partitions)
    else
      require(batches.forall(isIntKey), "groupByKey keys expects an Int32 key column")
      val keys = batches.map(batch => ColumnBatches.takeColumn(batch, 0))
      distinctBuckets(
        HashExchange.partitionAll(keys, 0, partitions, parallelism),
        parallelism,
        distinct,
      )

  /**
   * [[int32]] for a selection. Only present keys in `rows(0 until count)` are copied. A null value
   * is not read. An empty survivor list publishes `partitions` empty batches.
   */
  def int32Selected(
      selected: Vector[Selected],
      partitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(partitions > 0, s"partitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (selected.isEmpty) then empty(partitions)
    else
      require(selected.forall(one => isIntKey(one.batch)), "groupByKey keys expects an Int32 key")
      val dense = selected.flatMap(presentKeys)
      if (dense.isEmpty) then empty(partitions)
      else
        distinctBuckets(
          HashExchange.partitionAll(dense, 0, partitions, parallelism),
          parallelism,
          distinct,
        )

  /** [[int32]] for a `Long` key. The exchange is [[HashExchange.partitionAllInt64]]. */
  def int64(
      batches: Vector[ColumnBatch],
      partitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(partitions > 0, s"partitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (batches.isEmpty) then emptyLong(partitions)
    else
      require(batches.forall(isLongKey), "groupByKey keys expects an Int64 key column")
      val keys = batches.map(batch => ColumnBatches.takeColumn(batch, 0))
      distinctBuckets(
        HashExchange.partitionAllInt64(keys, 0, partitions, parallelism),
        parallelism,
        distinctLong,
      )

  /**
   * [[int64]] for a selection. Only present keys in `rows(0 until count)` are copied. A null value
   * is not read. An empty survivor list publishes `partitions` empty batches.
   */
  def int64Selected(
      selected: Vector[Selected],
      partitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(partitions > 0, s"partitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (selected.isEmpty) then emptyLong(partitions)
    else
      require(selected.forall(one => isLongKey(one.batch)), "groupByKey keys expects an Int64 key")
      val dense = selected.flatMap(presentLongKeys)
      if (dense.isEmpty) then emptyLong(partitions)
      else
        distinctBuckets(
          HashExchange.partitionAllInt64(dense, 0, partitions, parallelism),
          parallelism,
          distinctLong,
        )

  /** [[int32]] for a dictionary string key. The exchange is [[HashExchange.partitionAllUtf8]]. */
  def strings(
      batches: Vector[ColumnBatch],
      partitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(partitions > 0, s"partitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (batches.isEmpty) then emptyString(partitions)
    else
      require(batches.forall(isStringKey), "groupByKey keys expects a Utf8Dict key column")
      val keys = batches.map(batch => ColumnBatches.takeColumn(batch, 0))
      distinctBuckets(
        HashExchange.partitionAllUtf8(keys, 0, partitions, parallelism),
        parallelism,
        distinctUtf8,
      )

  /**
   * [[strings]] for a selection. Only present keys in `rows(0 until count)` are copied. `""` is
   * kept. A null value is not read. An empty survivor list publishes `partitions` empty batches.
   */
  def stringsSelected(
      selected: Vector[Selected],
      partitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(partitions > 0, s"partitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (selected.isEmpty) then emptyString(partitions)
    else
      require(
        selected.forall(one => isStringKey(one.batch)),
        "groupByKey keys expects a Utf8Dict key",
      )
      val dense = selected.flatMap(presentStringKeys)
      if (dense.isEmpty) then emptyString(partitions)
      else
        distinctBuckets(
          HashExchange.partitionAllUtf8(dense, 0, partitions, parallelism),
          parallelism,
          distinctUtf8,
        )

  private def distinctBuckets(
      buckets: Vector[ColumnBatch],
      parallelism: Int,
      one: ColumnBatch => ColumnBatch,
  ): Vector[ColumnBatch] =
    MorselScheduler.map(buckets, parallelism)((_, bucket) => one(bucket))

  private def distinct(batch: ColumnBatch): ColumnBatch =
    val keys = ColumnHash.int32At(batch, 0)
    val rows = keys.length
    if (rows == 0) then emptyInts
    else
      val capacity = ColumnHash.tableSize(rows)
      require(capacity >= 2 * rows, s"key table capacity $capacity is below 2 * $rows")
      val table = new KeyTable(capacity)
      val values = keys.values
      val words = keys.validity.words
      val present = ColumnHash.fullyPresent(keys)
      var row = 0
      while (row < rows) {
        if (present || Validity.isSet(words, row)) then table.add(values(row))
        row += 1
      }
      val groups = table.groups
      if (groups == 0) then emptyInts
      else
        val seen = new Array[Int](groups)
        var group = 0
        while (group < groups) {
          seen(group) = table.key(group)
          group += 1
        }
        val out = rowKeyOrder(seen)
        new ColumnBatch(
          Vector(LogicalType.Int32),
          Vector(Int32Column.of(out, Validity.allValid(groups), groups)),
          groups,
        )

  /** Present keys of one selection, in row-id order. A null key is omitted. */
  private def presentKeys(selected: Selected): Option[ColumnBatch] =
    val count = selected.count
    if (count == 0) then None
    else
      val keys = ColumnHash.int32At(selected.batch, 0)
      val out = new Array[Int](count)
      val ids = selected.rows
      val values = keys.values
      val words = keys.validity.words
      var slot = 0
      var written = 0
      while (slot < count) {
        val row = ids(slot)
        if (Validity.isSet(words, row)) then
          out(written) = values(row)
          written += 1
        slot += 1
      }
      if (written == 0) then None
      else Some(intBatch(Arrays.copyOf(out, written)))

  /** Present long keys of one selection, in row-id order. A null key is omitted. */
  private def presentLongKeys(selected: Selected): Option[ColumnBatch] =
    val count = selected.count
    if (count == 0) then None
    else
      val keys = ColumnHash.int64At(selected.batch, 0)
      val out = new Array[Long](count)
      val ids = selected.rows
      val values = keys.values
      val words = keys.validity.words
      var slot = 0
      var written = 0
      while (slot < count) {
        val row = ids(slot)
        if (Validity.isSet(words, row)) then
          out(written) = values(row)
          written += 1
        slot += 1
      }
      if (written == 0) then None
      else Some(longBatch(Arrays.copyOf(out, written)))

  /**
   * Present string keys of one selection, in row-id order. A null key is omitted. `""` is copied.
   * The dense batch shares the source dictionary.
   */
  private def presentStringKeys(selected: Selected): Option[ColumnBatch] =
    val count = selected.count
    if (count == 0) then None
    else
      val keys = utf8At(selected.batch, 0)
      val out = new Array[Int](count)
      val ids = selected.rows
      val codes = keys.codes
      val words = keys.validity.words
      var slot = 0
      var written = 0
      while (slot < count) {
        val row = ids(slot)
        if (Validity.isSet(words, row)) then
          out(written) = codes(row)
          written += 1
        slot += 1
      }
      if (written == 0) then None
      else Some(stringCodes(keys.dictionary, Arrays.copyOf(out, written)))

  private def distinctLong(batch: ColumnBatch): ColumnBatch =
    val keys = ColumnHash.int64At(batch, 0)
    val rows = keys.length
    if (rows == 0) then emptyLongs
    else
      val capacity = ColumnHash.tableSize(rows)
      require(capacity >= 2 * rows, s"key table capacity $capacity is below 2 * $rows")
      val table = new LongKeyTable(capacity)
      val values = keys.values
      val words = keys.validity.words
      val present = ColumnHash.fullyPresent(keys)
      var row = 0
      while (row < rows) {
        if (present || Validity.isSet(words, row)) then table.add(values(row))
        row += 1
      }
      val groups = table.groups
      if (groups == 0) then emptyLongs
      else
        val seen = new Array[Long](groups)
        var group = 0
        while (group < groups) {
          seen(group) = table.key(group)
          group += 1
        }
        longBatch(rowLongOrder(seen))

  /**
   * One post-exchange dictionary, so a code is one string. The published order is `groupMap` on
   * those strings, not code order.
   */
  private def distinctUtf8(batch: ColumnBatch): ColumnBatch =
    val keys = utf8At(batch, 0)
    val rows = keys.length
    if (rows == 0) then emptyStrings
    else
      val capacity = ColumnHash.tableSize(rows)
      require(capacity >= 2 * rows, s"key table capacity $capacity is below 2 * $rows")
      val table = new KeyTable(capacity)
      val codes = keys.codes
      val words = keys.validity.words
      val present = ColumnHash.fullyPresent(keys)
      var row = 0
      while (row < rows) {
        if (present || Validity.isSet(words, row)) then table.add(codes(row))
        row += 1
      }
      val groups = table.groups
      if (groups == 0) then emptyStrings
      else
        val dictionary = keys.dictionary
        val seen = new Array[String](groups)
        var group = 0
        while (group < groups) {
          seen(group) = dictionary(table.key(group))
          group += 1
        }
        stringBatch(rowStringOrder(seen))

  private def intBatch(values: Array[Int]): ColumnBatch =
    val rows = values.length
    new ColumnBatch(
      Vector(LogicalType.Int32),
      Vector(Int32Column.of(values, Validity.allValid(rows), rows)),
      rows,
    )

  private def isIntKey(batch: ColumnBatch): Boolean =
    batch.schema.headOption.contains(LogicalType.Int32)

  private def empty(partitions: Int): Vector[ColumnBatch] =
    Vector.fill(partitions)(emptyInts)

  private def emptyInts: ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32),
      Vector(Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0)),
      0,
    )

  private def longBatch(values: Array[Long]): ColumnBatch =
    val rows = values.length
    new ColumnBatch(
      Vector(LogicalType.Int64),
      Vector(Int64Column.of(values, Validity.allValid(rows), rows)),
      rows,
    )

  private def stringBatch(values: Array[String]): ColumnBatch =
    val column = DictUtf8Column.allValid(values)
    new ColumnBatch(Vector(LogicalType.Utf8Dict), Vector(column), values.length)

  /** Codes already belong to `dictionary`. The column shares that array. */
  private def stringCodes(dictionary: Array[String], codes: Array[Int]): ColumnBatch =
    val rows = codes.length
    val column = DictUtf8Column.of(dictionary, codes, Validity.allValid(rows), rows)
    new ColumnBatch(Vector(LogicalType.Utf8Dict), Vector(column), rows)

  private def isLongKey(batch: ColumnBatch): Boolean =
    batch.schema.headOption.contains(LogicalType.Int64)

  private def isStringKey(batch: ColumnBatch): Boolean =
    batch.schema.headOption.contains(LogicalType.Utf8Dict)

  private def emptyLong(partitions: Int): Vector[ColumnBatch] =
    Vector.fill(partitions)(emptyLongs)

  private def emptyString(partitions: Int): Vector[ColumnBatch] =
    Vector.fill(partitions)(emptyStrings)

  private def emptyLongs: ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int64),
      Vector(Int64Column.of(Array.emptyLongArray, Validity.allValid(0), 0)),
      0,
    )

  private def emptyStrings: ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Utf8Dict),
      Vector(DictUtf8Column.of(Array.empty[String], Array.emptyIntArray, Validity.allValid(0), 0)),
      0,
    )

  private def utf8At(batch: ColumnBatch, ordinal: Int): DictUtf8Column =
    ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, ordinal), ordinal)

  /**
   * Key order of `groupMap` on `firstSeen`. The row path inserts keys in this order, then `toSeq`
   * walks the immutable map. Duplicates are already removed.
   */
  private def rowKeyOrder(firstSeen: Array[Int]): Array[Int] =
    val ordered =
      ArraySeq.unsafeWrapArray(firstSeen).groupMap(key => key)((_: Int) => ()).keysIterator
    val out = new Array[Int](firstSeen.length)
    var index = 0
    while (ordered.hasNext && index < out.length) {
      out(index) = ordered.next()
      index += 1
    }
    require(index == out.length, s"groupMap published $index keys, expected ${out.length}")
    out

  /** [[rowKeyOrder]] for a `Long` key. Equality and the map hash are the full `Long`. */
  private def rowLongOrder(firstSeen: Array[Long]): Array[Long] =
    val ordered =
      ArraySeq.unsafeWrapArray(firstSeen).groupMap(key => key)((_: Long) => ()).keysIterator
    val out = new Array[Long](firstSeen.length)
    var index = 0
    while (ordered.hasNext && index < out.length) {
      out(index) = ordered.next()
      index += 1
    }
    require(index == out.length, s"groupMap published $index keys, expected ${out.length}")
    out

  /** [[rowKeyOrder]] for a `String` key. `"é"` and `"e\u0301"` stay distinct. */
  private def rowStringOrder(firstSeen: Array[String]): Array[String] =
    val ordered =
      ArraySeq.unsafeWrapArray(firstSeen).groupMap(key => key)((_: String) => ()).keysIterator
    val out = new Array[String](firstSeen.length)
    var index = 0
    while (ordered.hasNext && index < out.length) {
      out(index) = ordered.next()
      index += 1
    }
    require(index == out.length, s"groupMap published $index keys, expected ${out.length}")
    out

  /** Open-addressed first-seen keys. Insertion order is `slots(0 until groups)`. */
  private final class KeyTable(capacity: Int):
    private val mask: Int = capacity - 1
    private val limit: Int = capacity >>> 1
    private val keys: Array[Int] = new Array[Int](capacity)
    private val state: Array[Byte] = new Array[Byte](capacity)
    private val slots: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0

    def groups: Int = occupied

    def key(group: Int): Int = keys(slots(group))

    def add(key: Int): Unit =
      val slot = locate(key)
      if (state(slot) == 0) then
        if (occupied >= limit) then
          throw new IllegalStateException(s"groupByKey key table is full at $occupied keys")
        state(slot) = 1
        keys(slot) = key
        slots(occupied) = slot
        occupied += 1

    private def locate(key: Int): Int =
      var slot = ColumnHash.mix(key) & mask
      while (state(slot) != 0 && keys(slot) != key) {
        slot = (slot + 1) & mask
      }
      slot

  /**
   * Open-addressed first-seen `Long` keys. The stored key is the full `Long`. The probe mixes
   * `Long.hashCode`, so `1L << 32` and `1L` share a hash and stay distinct only because the key
   * itself is compared.
   */
  private final class LongKeyTable(capacity: Int):
    private val mask: Int = capacity - 1
    private val limit: Int = capacity >>> 1
    private val keys: Array[Long] = new Array[Long](capacity)
    private val state: Array[Byte] = new Array[Byte](capacity)
    private val slots: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0

    def groups: Int = occupied

    def key(group: Int): Long = keys(slots(group))

    def add(key: Long): Unit =
      val slot = locate(key)
      if (state(slot) == 0) then
        if (occupied >= limit) then
          throw new IllegalStateException(s"groupByKey key table is full at $occupied keys")
        state(slot) = 1
        keys(slot) = key
        slots(occupied) = slot
        occupied += 1

    private def locate(key: Long): Int =
      var slot = ColumnHash.mix(java.lang.Long.hashCode(key)) & mask
      while (state(slot) != 0 && keys(slot) != key) {
        slot = (slot + 1) & mask
      }
      slot
