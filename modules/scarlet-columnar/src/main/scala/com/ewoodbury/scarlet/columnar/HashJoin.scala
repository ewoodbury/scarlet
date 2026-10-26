package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Inner hash join of `Int32` keys. Null keys are dropped on both sides. A null value is preserved
 * on a matched row and stored as `0`.
 *
 * [[innerInt32]] builds from one batch and writes `(key, build value, probe value)` in probe
 * order. Within a probe row the build chain is newest-first. That order is part of the contract.
 *
 * [[openInt32]] fills one table from every build batch, then probes. A later batch is newer than
 * an earlier batch, and a later row is newer than an earlier row. `probe` writes
 * `(key, probe value, build value)`, so a broadcast join whose left input is the probe and whose
 * right input is the build publishes `(key, left, right)`. The table is not mutated by a probe, so
 * probes may run together. [[innerInt32]] does not use this table.
 *
 * Each side names its key and value ordinals. The probe writes matches directly, with the row
 * index in a `var`, so the hot loop stays allocation-free like `ColumnKernel`. Output columns keep
 * the grown capacity and `length` is the match count, so the result is not copied down to size.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object HashJoin:

  /** One side of the join. `key` and `value` are column ordinals in `batch`. */
  final case class JoinSide(batch: ColumnBatch, key: Int, value: Int)

  def innerInt32(build: JoinSide, probe: JoinSide): ColumnBatch =
    val buildKeys = ColumnHash.int32At(build.batch, build.key)
    val buildValues = ColumnHash.int32At(build.batch, build.value)
    val probeKeys = ColumnHash.int32At(probe.batch, probe.key)
    val probeValues = ColumnHash.int32At(probe.batch, probe.value)
    require(buildKeys.length == buildValues.length, "build key and value columns differ in length")
    require(probeKeys.length == probeValues.length, "probe key and value columns differ in length")
    if (buildKeys.length == 0 || probeKeys.length == 0) then emptyJoin
    else indexed(buildKeys, buildValues).write(probeKeys, probeValues)

  /**
   * One build table for every batch in `sides`, in vector order. An empty input, or batches whose
   * live lengths sum to zero, probes to an empty batch. A non-int column or a bad ordinal fails
   * before the table is built.
   */
  def openInt32(sides: Vector[JoinSide]): Int32Build =
    val checked = sides.map(checkedSide)
    val totalLong = checked.iterator.map(side => side.keys.length.toLong).sum
    require(totalLong <= Int.MaxValue.toLong, s"build side has $totalLong rows")
    val total = totalLong.toInt
    if (total == 0) then Int32Build.empty
    else
      val flat = copyBuild(checked, total)
      val table = Int32Build(flat)
      table.insert()
      table

  private def checkedSide(side: JoinSide): CheckedSide =
    val keys = ColumnHash.int32At(side.batch, side.key)
    val values = ColumnHash.int32At(side.batch, side.value)
    require(keys.length == values.length, "build key and value columns differ in length")
    CheckedSide(keys, values)

  private def copyBuild(sides: Vector[CheckedSide], total: Int): FlatBuild =
    val keys = new Array[Int](total)
    val values = new Array[Int](total)
    val keyWords = Validity.allocate(total)
    val valueWords = Validity.allocate(total)
    var cursor = 0
    sides.foreach { side =>
      val count = side.keys.length
      System.arraycopy(side.keys.values, 0, keys, cursor, count)
      System.arraycopy(side.values.values, 0, values, cursor, count)
      var row = 0
      while (row < count) {
        if (Validity.isSet(side.keys.validity.words, row)) then
          Validity.setBit(keyWords, cursor + row)
        if (Validity.isSet(side.values.validity.words, row)) then
          Validity.setBit(valueWords, cursor + row)
        row += 1
      }
      cursor += count
    }
    FlatBuild(total, keys, keyWords, values, valueWords)

  private final case class CheckedSide(keys: Int32Column, values: Int32Column)

  private final case class FlatBuild(
      rows: Int,
      keys: Array[Int],
      keyWords: Array[Long],
      values: Array[Int],
      valueWords: Array[Long],
  )

  private def indexed(buildKeys: Int32Column, buildValues: Int32Column): BuildTable =
    val table = new BuildTable(buildKeys, buildValues)
    table.insert()
    table

  private final class BuildTable(buildKeys: Int32Column, buildValues: Int32Column):
    private val rows: Int = buildKeys.length
    private val slotCapacity: Int = ColumnHash.tableSize(rows)
    private val mask: Int = slotCapacity - 1
    private val slotKey: Array[Int] = new Array[Int](slotCapacity)
    private val head: Array[Int] = new Array[Int](slotCapacity)
    private val next: Array[Int] = new Array[Int](rows)
    private val buildKeyValues: Array[Int] = buildKeys.values
    private val keyWords: Array[Long] = buildKeys.validity.words

    Arrays.fill(head, -1)
    Arrays.fill(next, -1)

    def insert(): Unit =
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then
          val key = buildKeyValues(row)
          var slot = ColumnHash.mix(key) & mask
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

    def write(probeKeys: Int32Column, probeValues: Int32Column): ColumnBatch =
      var outCapacity = math.max(16, probeKeys.length)
      var filled = 0
      var outKeys = new Array[Int](outCapacity)
      var outBuild = new Array[Int](outCapacity)
      var outProbe = new Array[Int](outCapacity)
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
          var slot = ColumnHash.mix(key) & mask
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

  private def publish(
      keys: Array[Int],
      buildValues: Array[Int],
      probeValues: Array[Int],
      buildWords: Array[Long],
      probeWords: Array[Long],
      capacity: Int,
      rows: Int,
  ): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keys, Validity.prefixValid(capacity, rows), rows),
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

  private def emptyJoin: ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
      ),
      0,
    )

  /**
   * Build table shared by every probe. `probe` writes `(key, probe value, build value)` in
   * probe-row order, newest build row first. An empty build returns an empty batch.
   */
  final class Int32Build private (
      mask: Int,
      slotKey: Array[Int],
      head: Array[Int],
      next: Array[Int],
      buildKeys: Array[Int],
      buildKeyWords: Array[Long],
      buildValues: Array[Int],
      buildValueWords: Array[Long],
      rows: Int,
  ):

    private[HashJoin] def insert(): Unit =
      var row = 0
      while (row < rows) {
        if (Validity.isSet(buildKeyWords, row)) then
          val key = buildKeys(row)
          var slot = ColumnHash.mix(key) & mask
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

    def probe(side: JoinSide): ColumnBatch =
      val probeKeys = ColumnHash.int32At(side.batch, side.key)
      val probeValues = ColumnHash.int32At(side.batch, side.value)
      require(
        probeKeys.length == probeValues.length,
        "probe key and value columns differ in length",
      )
      if (rows == 0 || probeKeys.length == 0) then emptyJoin
      else writeProbe(probeKeys, probeValues)

    private def writeProbe(probeKeys: Int32Column, probeValues: Int32Column): ColumnBatch =
      var outCapacity = math.max(16, probeKeys.length)
      var filled = 0
      var outKeys = new Array[Int](outCapacity)
      var outProbe = new Array[Int](outCapacity)
      var outBuild = new Array[Int](outCapacity)
      var probeWords = Validity.allocate(outCapacity)
      var buildWords = Validity.allocate(outCapacity)
      val probeValueValues = probeValues.values
      val probeValueWords = probeValues.validity.words
      val probeKeyValues = probeKeys.values
      val probeKeyWords = probeKeys.validity.words
      val probeRows = probeKeys.length
      var row = 0
      while (row < probeRows) {
        if (Validity.isSet(probeKeyWords, row)) then
          val key = probeKeyValues(row)
          var slot = ColumnHash.mix(key) & mask
          var resolved = false
          while (!resolved) {
            if (head(slot) == -1) then resolved = true
            else if (slotKey(slot) == key) then
              var buildRow = head(slot)
              while (buildRow != -1) {
                if (filled == outCapacity) then
                  outCapacity = doubled(outCapacity)
                  outKeys = Arrays.copyOf(outKeys, outCapacity)
                  outProbe = Arrays.copyOf(outProbe, outCapacity)
                  outBuild = Arrays.copyOf(outBuild, outCapacity)
                  probeWords = growWords(probeWords, outCapacity)
                  buildWords = growWords(buildWords, outCapacity)
                outKeys(filled) = buildKeys(buildRow)
                if (Validity.isSet(probeValueWords, row)) then
                  outProbe(filled) = probeValueValues(row)
                  Validity.setBit(probeWords, filled)
                if (Validity.isSet(buildValueWords, buildRow)) then
                  outBuild(filled) = buildValues(buildRow)
                  Validity.setBit(buildWords, filled)
                filled += 1
                buildRow = next(buildRow)
              }
              resolved = true
            else slot = (slot + 1) & mask
          }
        row += 1
      }
      if (filled == 0) then emptyJoin
      else publish(outKeys, outProbe, outBuild, probeWords, buildWords, outCapacity, filled)

  private object Int32Build:

    def empty: Int32Build =
      new Int32Build(
        mask = 0,
        slotKey = Array.emptyIntArray,
        head = Array.emptyIntArray,
        next = Array.emptyIntArray,
        buildKeys = Array.emptyIntArray,
        buildKeyWords = Array.emptyLongArray,
        buildValues = Array.emptyIntArray,
        buildValueWords = Array.emptyLongArray,
        rows = 0,
      )

    def apply(flat: FlatBuild): Int32Build =
      val slotCapacity = ColumnHash.tableSize(flat.rows)
      val head = new Array[Int](slotCapacity)
      val next = new Array[Int](flat.rows)
      Arrays.fill(head, -1)
      Arrays.fill(next, -1)
      new Int32Build(
        mask = slotCapacity - 1,
        slotKey = new Array[Int](slotCapacity),
        head = head,
        next = next,
        buildKeys = flat.keys,
        buildKeyWords = flat.keyWords,
        buildValues = flat.values,
        buildValueWords = flat.valueWords,
        rows = flat.rows,
      )
