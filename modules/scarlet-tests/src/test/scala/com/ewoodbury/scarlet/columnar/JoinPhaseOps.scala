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

  /**
   * Insert [[HashJoin]] uses for an in-memory build. An all-valid key column skips the bit test. A
   * null key stays out, matching the sparse arm.
   */
  def insertKernel(batch: ColumnBatch): Table =
    val keys = ColumnHash.int32At(batch, 0)
    val rows = keys.length
    val slots = new JoinSlots(rows, Long.MaxValue)
    val values = keys.values
    if (Validity.livePresent(keys.validity, rows)) then insertAll(slots, values, rows)
    else insertSparse(slots, values, keys.validity.words, rows)
    new Table(slots, rows)

  /** Longest chain in `table`. One means every inserted key is unique. */
  def longestChain(table: Table): Int = table.slots.maxChain

  /**
   * Writer [[HashJoin.innerInt32]] selects. `grouped` is [[JoinWrite.writeGrouped]], `dense` is
   * [[JoinWrite.writeDense]], and `row` is the per-row loop [[append]] copies. `empty` writes
   * nothing.
   */
  def writer(table: Table, build: ColumnBatch, probe: ColumnBatch): String =
    val probeKeys = ColumnHash.int32At(probe, 0)
    val probeValues = ColumnHash.int32At(probe, 1)
    val buildValues = ColumnHash.int32At(build, 1)
    val present = JoinWrite.dense(probeKeys, probeValues, buildValues)
    val probeRows = probeKeys.length
    if (table.rows == 0 || probeRows == 0) then "empty"
    else if (table.slots.maxChain > 1 && probeRows <= JoinWrite.MaxProbeRows && present) then
      "grouped"
    else if (present) then "dense"
    else "row"

  /**
   * Match columns [[HashJoin.innerInt32]] writes after insert. The reserve walk is included: the
   * kernel counts matches, then copies. `grouped`, `dense`, and `row` are the three arms.
   */
  def write(table: Table, build: ColumnBatch, probe: ColumnBatch): ColumnBatch =
    val reserved = reserve(table, probe)
    if (reserved == 0) then ColumnBatches.emptyIntJoin
    else
      writer(table, build, probe) match
        case "grouped" => appendGrouped(table, build, probe, reserved)
        case "dense" =>
          val probeKeys = ColumnHash.int32At(probe, 0)
          JoinWrite.writeDense(
            table.slots,
            ColumnHash.int32At(build, 0).values,
            ColumnHash.int32At(build, 1).values,
            probeKeys.values,
            ColumnHash.int32At(probe, 1).values,
            probeKeys.length,
            reserved,
            probeBeforeBuild = false,
          )
        case "row" => append(table, build, probe, reserved)
        case "empty" => ColumnBatches.emptyIntJoin
        case other =>
          throw new IllegalStateException(s"unknown join writer $other")

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
              outCapacity = ColumnHash.doubled(outCapacity)
              outKeys = Arrays.copyOf(outKeys, outCapacity)
              outBuild = Arrays.copyOf(outBuild, outCapacity)
              outProbe = Arrays.copyOf(outProbe, outCapacity)
              buildWords = Validity.growWords(buildWords, outCapacity)
              probeWords = Validity.growWords(probeWords, outCapacity)
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

  private def insertAll(slots: JoinSlots, values: Array[Int], rows: Int): Unit =
    var row = 0
    while (row < rows) {
      slots.insert(values(row), row)
      row += 1
    }

  private def insertSparse(
      slots: JoinSlots,
      values: Array[Int],
      words: Array[Long],
      rows: Int,
  ): Unit =
    var row = 0
    while (row < rows) {
      if (Validity.isSet(words, row)) then slots.insert(values(row), row)
      row += 1
    }

  /** Rows reserved before a write. Same cases as the kernel's match reserve, misses dropped. */
  private def reserve(table: Table, probe: ColumnBatch): Int =
    val keys = ColumnHash.int32At(probe, 0)
    val probeRows = keys.length
    if (probeRows == 0 || table.rows == 0) then 0
    else if (table.slots.maxChain > 1) then
      table.slots.countOutput(keys.values, keys.validity.words, probeRows, keepMisses = false)
    else if (table.slots.maxChain == 0) then 0
    else math.max(16, probeRows)

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
