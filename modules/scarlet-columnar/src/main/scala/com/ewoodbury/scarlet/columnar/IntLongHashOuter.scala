package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Hash outer joins of an `Int32` key and an `Int64` value on both sides.
 *
 * A null key is not inserted and does not match. The preserved side keeps a null key, and the
 * other value is absent. A null long is a clear validity bit. Absence is a different bit and means
 * the partner is missing. Output is `(key, left, right)`. Within a probe row the build chain is
 * newest-first. That walk is a kernel detail, not a public contract.
 *
 * This is not [[LongHashOuter]]. That kernel hashes a long key and throws when a value bit is
 * clear. This kernel does not spill.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Var",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Any",
  ),
)
object IntLongHashOuter:

  private enum Place:
    case Left, Right, Full

  def hashLeft(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    shuffled(
      left,
      right,
      numPartitions,
      parallelism,
      carryLeft = true,
      carryRight = false,
      Place.Left,
    )

  def hashRight(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    shuffled(
      left,
      right,
      numPartitions,
      parallelism,
      carryLeft = false,
      carryRight = true,
      Place.Right,
    )

  def hashFull(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    shuffled(
      left,
      right,
      numPartitions,
      parallelism,
      carryLeft = true,
      carryRight = true,
      Place.Full,
    )

  /** One left-outer batch. `left` is the probe and `right` is the build. */
  def leftOuter(left: ColumnBatch, right: ColumnBatch): ColumnBatch =
    join(left, right, Place.Left)

  /** One right-outer batch. `right` is the probe and `left` is the build. */
  def rightOuter(left: ColumnBatch, right: ColumnBatch): ColumnBatch =
    join(left, right, Place.Right)

  /**
   * One full-outer batch. A broadcast calls this once. Once per probe batch would emit unmatched
   * build rows again.
   */
  def fullOuter(left: ColumnBatch, right: ColumnBatch): ColumnBatch =
    join(left, right, Place.Full)

  /** Zero-row `(Int32, Int64, Int64)` batch. A fresh batch on every call. */
  private[columnar] def emptyBatch: ColumnBatch = empty

  /** Left outer rows. A clear bit with no absence mark is a null long, not a missing partner. */
  def decodeLeft(batch: ColumnBatch, maxRows: Int): Seq[(Int, (Long, Option[Long]))] =
    val (rows, keys, left, right) = columns(batch, maxRows)
    Seq.tabulate(rows) { row =>
      leftRow(intRef(keys, row), longRef(left, row), optionLong(right, row))
    }

  /** Right outer rows. A clear bit with no absence mark is a null long, not a missing partner. */
  def decodeRight(batch: ColumnBatch, maxRows: Int): Seq[(Int, (Option[Long], Long))] =
    val (rows, keys, left, right) = columns(batch, maxRows)
    Seq.tabulate(rows) { row =>
      rightRow(intRef(keys, row), optionLong(left, row), longRef(right, row))
    }

  /** Full outer rows. A clear bit with no absence mark is a null long, not a missing partner. */
  def decodeFull(batch: ColumnBatch, maxRows: Int): Seq[(Int, (Option[Long], Option[Long]))] =
    val (rows, keys, left, right) = columns(batch, maxRows)
    Seq.tabulate(rows) { row =>
      fullRow(intRef(keys, row), optionLong(left, row), optionLong(right, row))
    }

  private def shuffled(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      carryLeft: Boolean,
      carryRight: Boolean,
      place: Place,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"shuffle partitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (skip(left, right, place)) then Vector.fill(numPartitions)(empty)
    else
      val leftBuckets = buckets(left, numPartitions, parallelism, carryLeft)
      val rightBuckets = buckets(right, numPartitions, parallelism, carryRight)
      val probe = if (place == Place.Right) then rightBuckets else leftBuckets
      MorselScheduler.map(probe, parallelism) { (part, _) =>
        join(leftBuckets(part), rightBuckets(part), place)
      }

  /** Left outer with no left rows, or right outer with no right rows, emits nothing. */
  private def skip(left: Vector[ColumnBatch], right: Vector[ColumnBatch], place: Place): Boolean =
    (left.isEmpty && right.isEmpty) || (place == Place.Left && left.isEmpty) ||
      (place == Place.Right && right.isEmpty)

  private def buckets(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      carryNullKeys: Boolean,
  ): Vector[ColumnBatch] =
    if (batches.isEmpty) then Vector.fill(numPartitions)(empty)
    else
      HashExchange.partitionAll(
        batches,
        keyOrdinal = 0,
        numPartitions,
        parallelism,
        carryNullKeys = carryNullKeys,
      )

  private def join(left: ColumnBatch, right: ColumnBatch, place: Place): ColumnBatch =
    val probeIsLeft = place != Place.Right
    val keepBuild = place == Place.Full
    val probe = if (probeIsLeft) then left else right
    val build = if (probeIsLeft) then right else left
    val probeKeys = ColumnHash.int32At(probe, 0)
    val probeValues = ColumnHash.int64At(probe, 1)
    val buildKeys = ColumnHash.int32At(build, 0)
    val buildValues = ColumnHash.int64At(build, 1)
    require(probeKeys.length == probeValues.length, "probe key and value columns differ in length")
    require(buildKeys.length == buildValues.length, "build key and value columns differ in length")
    if (probeKeys.length == 0 && buildKeys.length == 0) then empty
    else if (probeKeys.length == 0 && !keepBuild) then empty
    else
      val table =
        if (buildKeys.length == 0) then None else Some(new BuildTable(buildKeys, buildValues))
      write(table, probeKeys, probeValues, buildKeys, buildValues, probeIsLeft, keepBuild)

  private final class BuildTable(buildKeys: Int32Column, buildValues: Int64Column):
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

    def slotOf(key: Int): Int = ColumnHash.mix(java.lang.Integer.hashCode(key)) & mask

    def nextSlot(slot: Int): Int = (slot + 1) & mask

    def headAt(slot: Int): Int = head(slot)

    def keyAt(slot: Int): Int = slotKey(slot)

    def nextAt(row: Int): Int = next(row)

  private def write(
      table: Option[BuildTable],
      probeKeys: Int32Column,
      probeValues: Int64Column,
      buildKeys: Int32Column,
      buildValues: Int64Column,
      probeIsLeft: Boolean,
      keepBuild: Boolean,
  ): ColumnBatch =
    var outCapacity = math.max(16, probeKeys.length + buildKeys.length)
    var filled = 0
    var outKeys = new Array[Int](outCapacity)
    var leftVals = new Array[Long](outCapacity)
    var rightVals = new Array[Long](outCapacity)
    var keyWords = Validity.allocate(outCapacity)
    var leftWords = Validity.allocate(outCapacity)
    var rightWords = Validity.allocate(outCapacity)
    var leftAbs = Validity.allocate(outCapacity)
    var rightAbs = Validity.allocate(outCapacity)
    val probeKeyValues = probeKeys.values
    val probeKeyWords = probeKeys.validity.words
    val probeValueValues = probeValues.values
    val probeValueWords = probeValues.validity.words
    val buildKeyValues = buildKeys.values
    val buildKeyWords = buildKeys.validity.words
    val buildValueValues = buildValues.values
    val buildValueWords = buildValues.validity.words
    val matched =
      if (keepBuild && buildKeys.length > 0) then Some(Validity.allocate(buildKeys.length))
      else None

    def grow(): Unit =
      if (filled == outCapacity) then
        outCapacity = ColumnHash.doubled(outCapacity)
        outKeys = Arrays.copyOf(outKeys, outCapacity)
        leftVals = Arrays.copyOf(leftVals, outCapacity)
        rightVals = Arrays.copyOf(rightVals, outCapacity)
        keyWords = Validity.growWords(keyWords, outCapacity)
        leftWords = Validity.growWords(leftWords, outCapacity)
        rightWords = Validity.growWords(rightWords, outCapacity)
        leftAbs = Validity.growWords(leftAbs, outCapacity)
        rightAbs = Validity.growWords(rightAbs, outCapacity)

    def add(
        key: Int,
        keyValid: Boolean,
        leftVal: Long,
        leftValid: Boolean,
        leftMiss: Boolean,
        rightVal: Long,
        rightValid: Boolean,
        rightMiss: Boolean,
    ): Unit =
      grow()
      outKeys(filled) = key
      if (keyValid) then Validity.setBit(keyWords, filled)
      if (leftValid) then
        leftVals(filled) = leftVal
        Validity.setBit(leftWords, filled)
      if (leftMiss) then Validity.setBit(leftAbs, filled)
      if (rightValid) then
        rightVals(filled) = rightVal
        Validity.setBit(rightWords, filled)
      if (rightMiss) then Validity.setBit(rightAbs, filled)
      filled += 1

    def emitMatch(probeRow: Int, buildRow: Int): Unit =
      val key = probeKeyValues(probeRow)
      if (probeIsLeft) then
        add(
          key,
          true,
          probeValueValues(probeRow),
          Validity.isSet(probeValueWords, probeRow),
          false,
          buildValueValues(buildRow),
          Validity.isSet(buildValueWords, buildRow),
          false,
        )
      else
        add(
          key,
          true,
          buildValueValues(buildRow),
          Validity.isSet(buildValueWords, buildRow),
          false,
          probeValueValues(probeRow),
          Validity.isSet(probeValueWords, probeRow),
          false,
        )

    def emitProbeMiss(probeRow: Int): Unit =
      val key = probeKeyValues(probeRow)
      val keyValid = Validity.isSet(probeKeyWords, probeRow)
      val value = probeValueValues(probeRow)
      val valid = Validity.isSet(probeValueWords, probeRow)
      if (probeIsLeft) then add(key, keyValid, value, valid, false, 0L, false, true)
      else add(key, keyValid, 0L, false, true, value, valid, false)

    def emitBuild(buildRow: Int, keyValid: Boolean): Unit =
      val key = buildKeyValues(buildRow)
      val value = buildValueValues(buildRow)
      val valid = Validity.isSet(buildValueWords, buildRow)
      if (probeIsLeft) then add(key, keyValid, 0L, false, true, value, valid, false)
      else add(key, keyValid, value, valid, false, 0L, false, true)

    table.foreach(_.insert())
    var row = 0
    while (row < probeKeys.length) {
      if (!Validity.isSet(probeKeyWords, row)) then emitProbeMiss(row)
      else
        table match
          case None => emitProbeMiss(row)
          case Some(indexed) =>
            val key = probeKeyValues(row)
            var slot = indexed.slotOf(key)
            var resolved = false
            var found = false
            while (!resolved) {
              if (indexed.headAt(slot) == -1) then resolved = true
              else if (indexed.keyAt(slot) == key) then
                found = true
                var buildRow = indexed.headAt(slot)
                while (buildRow != -1) {
                  emitMatch(row, buildRow)
                  matched.foreach(bits => Validity.setBit(bits, buildRow))
                  buildRow = indexed.nextAt(buildRow)
                }
                resolved = true
              else slot = indexed.nextSlot(slot)
            }
            if (!found) then emitProbeMiss(row)
      row += 1
    }
    if (keepBuild) then
      var buildRow = 0
      while (buildRow < buildKeys.length) {
        if (!Validity.isSet(buildKeyWords, buildRow)) then emitBuild(buildRow, false)
        else if (matched.forall(bits => !Validity.isSet(bits, buildRow))) then
          emitBuild(buildRow, true)
        buildRow += 1
      }
    if (filled == 0) then empty
    else
      publish(
        outKeys,
        leftVals,
        rightVals,
        keyWords,
        leftWords,
        rightWords,
        leftAbs,
        rightAbs,
        outCapacity,
        filled,
      )

  private def publish(
      keys: Array[Int],
      leftValues: Array[Long],
      rightValues: Array[Long],
      keyWords: Array[Long],
      leftWords: Array[Long],
      rightWords: Array[Long],
      leftAbs: Array[Long],
      rightAbs: Array[Long],
      capacity: Int,
      rows: Int,
  ): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int32Column.of(keys, Validity.fromWords(keyWords, capacity), rows),
        valueColumn(leftValues, leftWords, leftAbs, capacity, rows),
        valueColumn(rightValues, rightWords, rightAbs, capacity, rows),
      ),
      rows,
    )

  private def valueColumn(
      values: Array[Long],
      words: Array[Long],
      absence: Array[Long],
      capacity: Int,
      rows: Int,
  ): Int64Column =
    val validity = Validity.fromWords(words, capacity)
    if (Validity.anySet(absence, rows)) then
      Int64Column.withAbsence(values, validity, Validity.fromWords(absence, capacity), rows)
    else Int64Column.of(values, validity, rows)

  private def empty: ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int32Column.of(Array.empty[Int], Validity.allValid(0), 0),
        Int64Column.of(Array.empty[Long], Validity.allValid(0), 0),
        Int64Column.of(Array.empty[Long], Validity.allValid(0), 0),
      ),
      0,
    )

  private def decodedLength(batch: ColumnBatch, maxRows: Int): Int =
    require(maxRows >= 0, s"maximum decoded rows must be >= 0, got $maxRows")
    math.min(batch.length, maxRows)

  private def intRef(column: Int32Column, row: Int): java.lang.Integer =
    if (column.isValid(row)) java.lang.Integer.valueOf(column.values(row)) else null

  private def longRef(column: Int64Column, row: Int): java.lang.Long =
    if (column.isValid(row)) java.lang.Long.valueOf(column.values(row)) else null

  private def columns(
      batch: ColumnBatch,
      maxRows: Int,
  ): (Int, Int32Column, Int64Column, Int64Column) =
    val rows = decodedLength(batch, maxRows)
    val keys = ColumnKernel.asInt32(ColumnKernel.columnAt(batch, 0), 0)
    val left = ColumnKernel.asInt64(ColumnKernel.columnAt(batch, 1), 1)
    val right = ColumnKernel.asInt64(ColumnKernel.columnAt(batch, 2), 2)
    ColumnMetrics.decoded(rows)
    (rows, keys, left, right)

  /** Absence is [[scala.None]]. A clear bit with no absence mark stays a null inside [[Some]]. */
  private def optionLong(column: Int64Column, row: Int): Option[Long] =
    if (column.isAbsent(row)) then None
    else Some(longRef(column, row)).asInstanceOf[Option[Long]]

  private def leftRow(
      key: java.lang.Integer,
      left: java.lang.Long,
      right: Option[Long],
  ): (Int, (Long, Option[Long])) =
    Tuple2(key, Tuple2(left, right)).asInstanceOf[(Int, (Long, Option[Long]))]

  private def rightRow(
      key: java.lang.Integer,
      left: Option[Long],
      right: java.lang.Long,
  ): (Int, (Option[Long], Long)) =
    Tuple2(key, Tuple2(left, right)).asInstanceOf[(Int, (Option[Long], Long))]

  private def fullRow(
      key: java.lang.Integer,
      left: Option[Long],
      right: Option[Long],
  ): (Int, (Option[Long], Option[Long])) =
    Tuple2(key, Tuple2(left, right)).asInstanceOf[(Int, (Option[Long], Option[Long]))]
