package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Groups `(Int, Int)` pairs by key.
 *
 * [[HashExchange.partitionAll]] drops a null key and buckets with `floorMod(key, partitions)`, the
 * same rule `HashPartitioner` uses because `Int.hashCode` is the value. Each bucket keeps
 * first-seen key order and input value order. A null value is not dropped. A clear value bit is a
 * planner bug and throws `IllegalArgumentException`, including a null value on a row whose key is
 * also null. That check runs before the exchange, which would otherwise drop the null key and hide
 * the value. Classification and `copiedIntPairs` only produce a fully present value column. One
 * [[GroupedIntBatch]] is published per partition, including an empty bucket. There is no partial
 * combine and no spill.
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

  private def requirePresentValues(batch: ColumnBatch): Unit =
    val values = ColumnHash.int32At(batch, 1)
    require(
      Validity.livePresent(values.validity, values.length),
      "groupByKey value column has a null; a clear value bit is a planner bug",
    )

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
    val table = new GroupTable(ColumnHash.tableSize(rows))
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
