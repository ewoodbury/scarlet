package com.ewoodbury.scarlet.columnar

import java.util.Arrays

import scala.collection.immutable.ArraySeq

/**
 * Distinct int keys of a `groupByKey` shuffle.
 *
 * The exchange is the same raw [[HashExchange.partitionAll]] as [[GroupByKey.int32]]. A null key
 * is dropped, the bucket is `floorMod(key, partitions)`, and order inside a bucket is input order.
 * Each bucket then emits keys in the same order as the row path's `groupMap(...).toSeq`, which
 * follows the immutable map built from `mutable.Map`, not insertion order and not a sort. No value
 * array is allocated. This is not [[HashCount]] and not [[DistinctKernel]]: the count kernel
 * partial-combines, and distinct keeps a null. [[GroupByKey.valueListsBuilt]] stays unchanged.
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
      distinctBuckets(HashExchange.partitionAll(keys, 0, partitions, parallelism), parallelism)

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
        distinctBuckets(HashExchange.partitionAll(dense, 0, partitions, parallelism), parallelism)

  private def distinctBuckets(
      buckets: Vector[ColumnBatch],
      parallelism: Int,
  ): Vector[ColumnBatch] =
    MorselScheduler.map(buckets, parallelism)((_, bucket) => distinct(bucket))

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
