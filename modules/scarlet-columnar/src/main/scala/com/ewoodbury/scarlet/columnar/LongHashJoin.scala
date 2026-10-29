package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Inner hash join of `Int64` keys and `Int64` values.
 *
 * The table is the int shuffle-hash shape: open addressing on the key, flat key and value arrays,
 * and a chain so duplicate keys emit the full cartesian product. [[innerInt64]] builds the left
 * side and probes the right. It writes `(key, left, right)`. Within a probe row the build chain is
 * newest-first. That walk is a kernel detail, not a public contract.
 *
 * A null key is not inserted and is not probed, so a stored `0` does not match key `0`. Every
 * value cell must be present. The interpreter refuses a null value, so the plan stays on the row
 * path. There is no spill and no build budget. The probe writes matches directly. Output columns
 * keep the grown capacity and `length` is the match count.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object LongHashJoin:

  def innerInt64(build: HashJoin.JoinSide, probe: HashJoin.JoinSide): ColumnBatch =
    val buildKeys = ColumnHash.int64At(build.batch, build.key)
    val buildValues = ColumnHash.int64At(build.batch, build.value)
    val probeKeys = ColumnHash.int64At(probe.batch, probe.key)
    val probeValues = ColumnHash.int64At(probe.batch, probe.value)
    require(buildKeys.length == buildValues.length, "build key and value columns differ in length")
    require(probeKeys.length == probeValues.length, "probe key and value columns differ in length")
    if (buildKeys.length == 0 || probeKeys.length == 0) then emptyJoin
    else indexed(buildKeys, buildValues).write(probeKeys, probeValues)

  private def indexed(buildKeys: Int64Column, buildValues: Int64Column): BuildTable =
    val table = new BuildTable(buildKeys, buildValues)
    table.insert()
    table

  private final class BuildTable(buildKeys: Int64Column, buildValues: Int64Column):
    private val rows: Int = buildKeys.length
    private val slotCapacity: Int = ColumnHash.tableSize(rows)
    private val mask: Int = slotCapacity - 1
    private val slotKey: Array[Long] = new Array[Long](slotCapacity)
    private val head: Array[Int] = new Array[Int](slotCapacity)
    private val next: Array[Int] = new Array[Int](rows)
    private val buildKeyValues: Array[Long] = buildKeys.values
    private val keyWords: Array[Long] = buildKeys.validity.words

    Arrays.fill(head, -1)
    Arrays.fill(next, -1)

    def insert(): Unit =
      var row = 0
      while (row < rows) {
        // A null key stays out of the table. Its payload is 0, and inserting it would match
        // probe key 0.
        if (Validity.isSet(keyWords, row)) then
          val key = buildKeyValues(row)
          var slot = slotOf(key)
          var placed = false
          while (!placed) {
            if (head(slot) == -1) then
              head(slot) = row
              slotKey(slot) = key
              placed = true
            else if (slotKey(slot) == key) then
              next(row) = head(slot)
              head(slot) = row
              placed = true
            else slot = (slot + 1) & mask
          }
        row += 1
      }

    def write(probeKeys: Int64Column, probeValues: Int64Column): ColumnBatch =
      var outCapacity = math.max(16, probeKeys.length)
      var filled = 0
      var outKeys = new Array[Long](outCapacity)
      var outBuild = new Array[Long](outCapacity)
      var outProbe = new Array[Long](outCapacity)
      var buildWords = Validity.allocate(outCapacity)
      var probeWords = Validity.allocate(outCapacity)
      val buildValueValues = buildValues.values
      val buildValueWords = buildValues.validity.words
      val probeValueValues = probeValues.values
      val probeValueWords = probeValues.validity.words
      val probeKeyValues = probeKeys.values
      val probeKeyWords = probeKeys.validity.words
      val probeRows = probeKeys.length
      var row = 0
      while (row < probeRows) {
        if (Validity.isSet(probeKeyWords, row)) then
          val key = probeKeyValues(row)
          var slot = slotOf(key)
          var resolved = false
          while (!resolved) {
            if (head(slot) == -1) then resolved = true
            else if (slotKey(slot) == key) then
              var buildRow = head(slot)
              while (buildRow != -1) {
                if (filled == outCapacity) then
                  outCapacity = doubled(outCapacity)
                  outKeys = Arrays.copyOf(outKeys, outCapacity)
                  outBuild = Arrays.copyOf(outBuild, outCapacity)
                  outProbe = Arrays.copyOf(outProbe, outCapacity)
                  buildWords = growWords(buildWords, outCapacity)
                  probeWords = growWords(probeWords, outCapacity)
                outKeys(filled) = buildKeyValues(buildRow)
                if (Validity.isSet(buildValueWords, buildRow)) then
                  outBuild(filled) = buildValueValues(buildRow)
                  Validity.setBit(buildWords, filled)
                if (Validity.isSet(probeValueWords, row)) then
                  outProbe(filled) = probeValueValues(row)
                  Validity.setBit(probeWords, filled)
                filled += 1
                buildRow = next(buildRow)
              }
              resolved = true
            else slot = (slot + 1) & mask
          }
        row += 1
      }
      if (filled == 0) then emptyJoin
      else publish(outKeys, outBuild, outProbe, buildWords, probeWords, outCapacity, filled)

    private def slotOf(key: Long): Int =
      ColumnHash.mix(java.lang.Long.hashCode(key)) & mask

  private def publish(
      keys: Array[Long],
      buildValues: Array[Long],
      probeValues: Array[Long],
      buildWords: Array[Long],
      probeWords: Array[Long],
      capacity: Int,
      rows: Int,
  ): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int64Column.of(keys, Validity.prefixValid(capacity, rows), rows),
        Int64Column.of(buildValues, Validity.fromWords(buildWords, capacity), rows),
        Int64Column.of(probeValues, Validity.fromWords(probeWords, capacity), rows),
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

  private def emptyJoin: ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int64Column.of(Array.emptyLongArray, Validity.allValid(0), 0),
        Int64Column.of(Array.emptyLongArray, Validity.allValid(0), 0),
        Int64Column.of(Array.emptyLongArray, Validity.allValid(0), 0),
      ),
      0,
    )
