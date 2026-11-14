package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Split of one shuffle-hash partition for [[com.ewoodbury.scarlet.execution.JoinPhaseBench]].
 *
 * `insert`, `walk`, and `append` follow the per-row writer: the same slot table, the same match
 * count, and a capacity test plus two validity updates on every output row. `appendGrouped` calls
 * [[JoinWrite]]. Not a test, and not part of `make test`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object JoinPhaseOps:

  final class Table(val slots: JoinSlots, val rows: Int)

  def insert(batch: ColumnBatch): Table =
    val keys = ColumnHash.int32At(batch, 0)
    val rows = keys.length
    val slots = new JoinSlots(rows, Long.MaxValue)
    val values = keys.values
    val words = keys.validity.words
    var row = 0
    while (row < rows) {
      if (Validity.isSet(words, row)) then slots.insert(values(row), row)
      row += 1
    }
    new Table(slots, rows)

  /** Probe rows that hit, counted by chain length. Does not write. */
  def walk(table: Table, probe: ColumnBatch): Int =
    val keys = ColumnHash.int32At(probe, 0)
    table.slots.countOutput(keys.values, keys.validity.words, keys.length, keepMisses = false)

  /** Current writer: a capacity test and two validity updates on every output row. */
  def append(table: Table, build: ColumnBatch, probe: ColumnBatch, reserved: Int): ColumnBatch =
    val buildKeys = ColumnHash.int32At(build, 0)
    val buildValues = ColumnHash.int32At(build, 1)
    val probeKeys = ColumnHash.int32At(probe, 0)
    val probeValues = ColumnHash.int32At(probe, 1)
    val buildKeyValues = buildKeys.values
    val buildValueValues = buildValues.values
    val buildValueWords = buildValues.validity.words
    val probeKeyValues = probeKeys.values
    val probeKeyWords = probeKeys.validity.words
    val probeValueValues = probeValues.values
    val probeValueWords = probeValues.validity.words
    var outCapacity = reserved
    var filled = 0
    var outKeys = new Array[Int](outCapacity)
    var outBuild = new Array[Int](outCapacity)
    var outProbe = new Array[Int](outCapacity)
    var buildWords = Validity.allocate(outCapacity)
    var probeWords = Validity.allocate(outCapacity)
    var row = 0
    while (row < probeKeys.length) {
      if (Validity.isSet(probeKeyWords, row)) then
        val slot = table.slots.locate(probeKeyValues(row))
        if (slot >= 0) then
          var buildRow = table.slots.headRow(slot)
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
            buildRow = table.slots.nextRow(buildRow)
          }
      row += 1
    }
    publish(outKeys, outBuild, outProbe, buildWords, probeWords, outCapacity, filled)

  /** The kernel's dense writer: one scratch run per chain, copied to each hit. */
  def appendGrouped(
      table: Table,
      build: ColumnBatch,
      probe: ColumnBatch,
      reserved: Int,
  ): ColumnBatch =
    JoinWrite.writeGrouped(
      table.slots,
      ColumnHash.int32At(build, 1).values,
      ColumnHash.int32At(probe, 0),
      ColumnHash.int32At(probe, 1),
      reserved,
    )

  def kernel(build: ColumnBatch, probe: ColumnBatch): ColumnBatch =
    HashJoin.innerInt32(
      HashJoin.JoinSide(build, key = 0, value = 1),
      HashJoin.JoinSide(probe, key = 0, value = 1),
    )

  def liveValues(batch: ColumnBatch): Boolean =
    val rows = batch.length
    Validity.livePresent(ColumnHash.int32At(batch, 0).validity, rows) &&
    Validity.livePresent(ColumnHash.int32At(batch, 1).validity, rows)

  /** Live payload and validity words, including spare capacity past `length`. */
  def same(left: ColumnBatch, right: ColumnBatch): Boolean =
    left.length == right.length &&
      (0 until 3).forall { column =>
        val a = ColumnHash.int32At(left, column)
        val b = ColumnHash.int32At(right, column)
        a.values.length == b.values.length &&
        Arrays.equals(a.values, 0, left.length, b.values, 0, right.length) &&
        Arrays.equals(a.validity.words, b.validity.words)
      }

  def fold(batch: ColumnBatch): Long =
    if (batch.length == 0) then 0L
    else
      val keys = ColumnHash.int32At(batch, 0)
      val left = ColumnHash.int32At(batch, 1)
      val right = ColumnHash.int32At(batch, 2)
      var row = 0
      var hash = 0L
      while (row < batch.length) {
        val key = keys.values(row)
        val buildValue = if (left.isValid(row)) then left.values(row) else 0
        val probeValue = if (right.isValid(row)) then right.values(row) else 0
        hash += (key.toLong * 31L) ^ buildValue.toLong ^ probeValue.toLong
        row += 1
      }
      hash

  private def publish(
      keys: Array[Int],
      buildValues: Array[Int],
      probeValues: Array[Int],
      buildWords: Array[Long],
      probeWords: Array[Long],
      capacity: Int,
      rows: Int,
  ): ColumnBatch =
    if (rows == 0) then ColumnBatches.emptyIntJoin
    else
      triple(
        keys,
        buildValues,
        probeValues,
        Validity.prefixValid(capacity, rows),
        Validity.fromWords(buildWords, capacity),
        Validity.fromWords(probeWords, capacity),
        rows,
      )

  private def triple(
      keys: Array[Int],
      buildValues: Array[Int],
      probeValues: Array[Int],
      keyBits: Validity,
      buildBits: Validity,
      probeBits: Validity,
      rows: Int,
  ): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keys, keyBits, rows),
        Int32Column.of(buildValues, buildBits, rows),
        Int32Column.of(probeValues, probeBits, rows),
      ),
      rows,
    )

  private def doubled(capacity: Int): Int =
    if (capacity >= ColumnHash.MaxCapacity) then
      throw new IllegalStateException(s"join output reached $capacity rows")
    capacity << 1

  private def growWords(words: Array[Long], rowCapacity: Int): Array[Long] =
    val needed = (rowCapacity + 63) >>> 6
    if (words.length == needed) then words else Arrays.copyOf(words, needed)
