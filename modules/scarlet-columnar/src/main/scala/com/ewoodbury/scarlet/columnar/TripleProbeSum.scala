package com.ewoodbury.scarlet.columnar

import scala.annotation.unused

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Grouped sum of a triple project, updated from an int inner probe.
 *
 * The build is the left input and the probe is the right, the same sides as
 * [[HashJoin.innerInt32]]. A null key is not inserted and is not a probe hit. Each hit calls
 * [[TripleToIntLong.offer]] on `(probe key, build value, probe value)` and the projected pair is
 * added under that key. The match columns are not written, and [[JoinWrite]] is not called.
 *
 * A null value on a hit declines before the function runs, the same refusal as
 * [[TripleToIntLong.project]]. A result that is not `(Int, Long)` declines from
 * [[TripleToIntLong]]. An empty build or probe publishes an empty sum and does not call the
 * function.
 *
 * [[visit]] runs the same hits and discards the pairs, so a reducer that is not addition can still
 * name a bad cell. It does not publish a sum. [[count]] offers each projected key into
 * [[HashCount.partialKeys]] and does not write the match columns. [[countKept]] drops a hit that
 * fails one triple predicate before the map runs. The predicate body is not read. A key or value
 * slot keeps the projected cell after the offer. A null projected key is dropped and the key test
 * is not called.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Var",
  ),
)
private[scarlet] object TripleProbeSum:

  def sum(
      build: HashJoin.JoinSide,
      probe: HashJoin.JoinSide,
      mapper: TripleToIntLong.TripleMap,
  ): ColumnBatch =
    val columns = sides(build, probe)
    if (columns.buildKeys.length == 0 || columns.probeKeys.length == 0) then
      ColumnBatches.emptyIntLongSum
    else
      val slots = index(columns.buildKeys)
      HashAggregate.consume { addSum =>
        val sink = new TripleToIntLong.LongSink:
          def add(key: Int, value: Long): Unit = addSum(key, value)
        walk(slots, columns, mapper, sink)
        ColumnMetrics.fused()
      }

  /**
   * One `(key, count)` partial for this probe. Each hit offers the projected key. The long cell is
   * checked and dropped. An empty side, or a probe that misses, does not call the function. A
   * match table that does not fit the spill budget declines before any key is inserted. `slot`
   * keeps a projected key or value after the offer. A null projected key is dropped and the key
   * test is not called. [[JoinWrite]] is not called.
   */
  def count(
      build: HashJoin.JoinSide,
      probe: HashJoin.JoinSide,
      mapper: TripleToIntLong.TripleMap,
      slot: Option[TripleToIntLong.IntLongSlot],
  ): ColumnBatch =
    partial(build, probe) { (slots, columns, sink) =>
      slot match
        case None => walk(slots, columns, mapper, sink)
        case Some(keep) => walkSlot(slots, columns, mapper, keep, sink)
    }

  /**
   * [[count]] after one predicate. The match estimate still counts every key hit, including rows
   * [[keep]] will drop. A null component is dropped when [[dropNull]] is set, and [[keep]] is not
   * called. Otherwise a null hit declines before [[keep]] runs.
   */
  def countKept(
      build: HashJoin.JoinSide,
      probe: HashJoin.JoinSide,
      mapper: TripleToIntLong.TripleMap,
      keep: PairKernel.IntTriplePredicate,
      dropNull: Boolean,
  ): ColumnBatch =
    partial(build, probe) { (slots, columns, sink) =>
      walkKept(slots, columns, mapper, sink, keep, dropNull)
    }

  private def partial(
      build: HashJoin.JoinSide,
      probe: HashJoin.JoinSide,
  )(offer: (JoinSlots, Sides, TripleToIntLong.LongSink) => Unit): ColumnBatch =
    val columns = sides(build, probe)
    if (columns.buildKeys.length == 0 || columns.probeKeys.length == 0) then
      ColumnBatches.emptyIntPairs
    else
      val slots = index(columns.buildKeys)
      val matches = matchRows(slots, columns)
      if (matches == 0) then ColumnBatches.emptyIntPairs
      else if (overBudget(matches)) then declineBudget()
      else
        val hint = math.min(matches, columns.probeKeys.length)
        HashCount.partialKeys(hint) { addKey =>
          val sink = new TripleToIntLong.LongSink:
            def add(key: Int, @unused value: Long): Unit = addKey(key)
          offer(slots, columns, sink)
          ColumnMetrics.fused()
        }

  /** Every hit the sum would add. The pairs are checked and dropped. */
  def visit(
      build: HashJoin.JoinSide,
      probe: HashJoin.JoinSide,
      mapper: TripleToIntLong.TripleMap,
  ): Unit =
    val columns = sides(build, probe)
    if (columns.buildKeys.length != 0 && columns.probeKeys.length != 0) then
      walk(index(columns.buildKeys), columns, mapper, Drop)

  private final case class Sides(
      buildKeys: Int32Column,
      buildValues: Int32Column,
      probeKeys: Int32Column,
      probeValues: Int32Column,
  )

  private def sides(build: HashJoin.JoinSide, probe: HashJoin.JoinSide): Sides =
    val buildKeys = ColumnHash.int32At(build.batch, build.key)
    val buildValues = ColumnHash.int32At(build.batch, build.value)
    val probeKeys = ColumnHash.int32At(probe.batch, probe.key)
    val probeValues = ColumnHash.int32At(probe.batch, probe.value)
    require(buildKeys.length == buildValues.length, "build key and value columns differ in length")
    require(probeKeys.length == probeValues.length, "probe key and value columns differ in length")
    Sides(buildKeys, buildValues, probeKeys, probeValues)

  private def index(keys: Int32Column): JoinSlots =
    val rows = keys.length
    val slots = new JoinSlots(rows, Long.MaxValue)
    val values = keys.values
    var row = 0
    if (Validity.livePresent(keys.validity, rows)) then
      while (row < rows) {
        slots.insert(values(row), row)
        row += 1
      }
    else
      val words = keys.validity.words
      while (row < rows) {
        if (Validity.isSet(words, row)) then slots.insert(values(row), row)
        row += 1
      }
    slots

  /** Chain steps a present probe key would offer. A null probe key is not a hit. */
  private def matchRows(slots: JoinSlots, columns: Sides): Int =
    val probeRows = columns.probeKeys.length
    val probeKeys = columns.probeKeys.values
    val dense = Validity.livePresent(columns.probeKeys.validity, probeRows)
    val words = columns.probeKeys.validity.words
    var row = 0
    var matches = 0L
    while (row < probeRows) {
      val keyOk = dense || Validity.isSet(words, row)
      if (keyOk) then
        val slot = slots.locate(probeKeys(row))
        if (slot >= 0) then matches += slots.chainAt(slot).toLong
      row += 1
    }
    if (matches > Int.MaxValue.toLong) then declineBudget()
    matches.toInt

  private def overBudget(matches: Int): Boolean =
    HashCount.inputRowBytes(matches.toLong) > ScarletConf.get.spillBudgetBytes

  private def declineBudget(): Nothing =
    throw new ColumnarFallback(
      "a projected join count over the spill budget stays on the row path",
    )

  private def walk(
      slots: JoinSlots,
      columns: Sides,
      mapper: TripleToIntLong.TripleMap,
      sink: TripleToIntLong.LongSink,
  ): Unit =
    val probeRows = columns.probeKeys.length
    val present =
      Validity.livePresent(columns.probeKeys.validity, probeRows) &&
        Validity.livePresent(columns.probeValues.validity, probeRows) &&
        Validity.livePresent(columns.buildValues.validity, columns.buildValues.length)
    if (present) then walkPresent(slots, columns, probeRows, mapper, sink)
    else walkSparse(slots, columns, probeRows, mapper, sink)

  /** Every live probe key, probe value, and build value is present. No bit test in the loop. */
  private def walkPresent(
      slots: JoinSlots,
      columns: Sides,
      probeRows: Int,
      mapper: TripleToIntLong.TripleMap,
      sink: TripleToIntLong.LongSink,
  ): Unit =
    val left = columns.buildValues.values
    val probeKeys = columns.probeKeys.values
    val right = columns.probeValues.values
    var row = 0
    while (row < probeRows) {
      val key = probeKeys(row)
      val slot = slots.locate(key)
      if (slot >= 0) then
        val probe = right(row)
        var buildRow = slots.headRow(slot)
        while (buildRow >= 0) {
          TripleToIntLong.offer(mapper(key, left(buildRow), probe), sink)
          buildRow = slots.nextRow(buildRow)
        }
      row += 1
    }

  /** A null probe key is not a hit. A null value on a hit declines before the function runs. */
  private def walkSparse(
      slots: JoinSlots,
      columns: Sides,
      probeRows: Int,
      mapper: TripleToIntLong.TripleMap,
      sink: TripleToIntLong.LongSink,
  ): Unit =
    val probeKeys = columns.probeKeys.values
    val probeKeyWords = columns.probeKeys.validity.words
    val left = columns.buildValues.values
    val leftWords = columns.buildValues.validity.words
    val right = columns.probeValues.values
    val rightWords = columns.probeValues.validity.words
    val probeKeysDense = Validity.livePresent(columns.probeKeys.validity, probeRows)
    val leftDense = Validity.livePresent(columns.buildValues.validity, columns.buildValues.length)
    val rightDense = Validity.livePresent(columns.probeValues.validity, probeRows)
    var row = 0
    while (row < probeRows) {
      val keyOk = probeKeysDense || Validity.isSet(probeKeyWords, row)
      if (keyOk) then
        val key = probeKeys(row)
        val slot = slots.locate(key)
        if (slot >= 0) then
          val probeOk = rightDense || Validity.isSet(rightWords, row)
          var buildRow = slots.headRow(slot)
          while (buildRow >= 0) {
            val buildOk = leftDense || Validity.isSet(leftWords, buildRow)
            if (!probeOk || !buildOk) then TripleToIntLong.rejectNull()
            TripleToIntLong.offer(mapper(key, left(buildRow), right(row)), sink)
            buildRow = slots.nextRow(buildRow)
          }
      row += 1
    }

  private def walkKept(
      slots: JoinSlots,
      columns: Sides,
      mapper: TripleToIntLong.TripleMap,
      sink: TripleToIntLong.LongSink,
      keep: PairKernel.IntTriplePredicate,
      dropNull: Boolean,
  ): Unit =
    val probeRows = columns.probeKeys.length
    val present =
      Validity.livePresent(columns.probeKeys.validity, probeRows) &&
        Validity.livePresent(columns.probeValues.validity, probeRows) &&
        Validity.livePresent(columns.buildValues.validity, columns.buildValues.length)
    if (present) then walkKeptPresent(slots, columns, probeRows, mapper, sink, keep)
    else walkKeptSparse(slots, columns, probeRows, mapper, sink, keep, dropNull)

  /** Every live probe key, probe value, and build value is present. No bit test in the loop. */
  private def walkKeptPresent(
      slots: JoinSlots,
      columns: Sides,
      probeRows: Int,
      mapper: TripleToIntLong.TripleMap,
      sink: TripleToIntLong.LongSink,
      keep: PairKernel.IntTriplePredicate,
  ): Unit =
    val left = columns.buildValues.values
    val probeKeys = columns.probeKeys.values
    val right = columns.probeValues.values
    var row = 0
    while (row < probeRows) {
      val key = probeKeys(row)
      val slot = slots.locate(key)
      if (slot >= 0) then
        val probe = right(row)
        var buildRow = slots.headRow(slot)
        while (buildRow >= 0) {
          val build = left(buildRow)
          if (keep(key, build, probe)) then TripleToIntLong.offer(mapper(key, build, probe), sink)
          buildRow = slots.nextRow(buildRow)
        }
      row += 1
    }

  /** A null component is dropped, or declined, before [[keep]] and the map. */
  private def walkKeptSparse(
      slots: JoinSlots,
      columns: Sides,
      probeRows: Int,
      mapper: TripleToIntLong.TripleMap,
      sink: TripleToIntLong.LongSink,
      keep: PairKernel.IntTriplePredicate,
      dropNull: Boolean,
  ): Unit =
    val probeKeys = columns.probeKeys.values
    val probeKeyWords = columns.probeKeys.validity.words
    val left = columns.buildValues.values
    val leftWords = columns.buildValues.validity.words
    val right = columns.probeValues.values
    val rightWords = columns.probeValues.validity.words
    val probeKeysDense = Validity.livePresent(columns.probeKeys.validity, probeRows)
    val leftDense = Validity.livePresent(columns.buildValues.validity, columns.buildValues.length)
    val rightDense = Validity.livePresent(columns.probeValues.validity, probeRows)
    var row = 0
    while (row < probeRows) {
      val keyOk = probeKeysDense || Validity.isSet(probeKeyWords, row)
      if (keyOk) then
        val key = probeKeys(row)
        val slot = slots.locate(key)
        if (slot >= 0) then
          val probeOk = rightDense || Validity.isSet(rightWords, row)
          var buildRow = slots.headRow(slot)
          while (buildRow >= 0) {
            val buildOk = leftDense || Validity.isSet(leftWords, buildRow)
            if (dropNull) then
              if (probeOk && buildOk && keep(key, left(buildRow), right(row))) then
                TripleToIntLong.offer(mapper(key, left(buildRow), right(row)), sink)
            else
              if (!probeOk || !buildOk) then TripleToIntLong.rejectNull()
              if (keep(key, left(buildRow), right(row))) then
                TripleToIntLong.offer(mapper(key, left(buildRow), right(row)), sink)
            buildRow = slots.nextRow(buildRow)
          }
      row += 1
    }

  /** [[walk]] plus one slot on the projected pair. The input null check is unchanged. */
  private def walkSlot(
      slots: JoinSlots,
      columns: Sides,
      mapper: TripleToIntLong.TripleMap,
      slot: TripleToIntLong.IntLongSlot,
      sink: TripleToIntLong.LongSink,
  ): Unit =
    val probeRows = columns.probeKeys.length
    val present =
      Validity.livePresent(columns.probeKeys.validity, probeRows) &&
        Validity.livePresent(columns.probeValues.validity, probeRows) &&
        Validity.livePresent(columns.buildValues.validity, columns.buildValues.length)
    if (present) then walkSlotPresent(slots, columns, probeRows, mapper, slot, sink)
    else walkSlotSparse(slots, columns, probeRows, mapper, slot, sink)

  private def walkSlotPresent(
      slots: JoinSlots,
      columns: Sides,
      probeRows: Int,
      mapper: TripleToIntLong.TripleMap,
      slot: TripleToIntLong.IntLongSlot,
      sink: TripleToIntLong.LongSink,
  ): Unit =
    val left = columns.buildValues.values
    val probeKeys = columns.probeKeys.values
    val right = columns.probeValues.values
    var row = 0
    while (row < probeRows) {
      val key = probeKeys(row)
      val located = slots.locate(key)
      if (located >= 0) then
        val probe = right(row)
        var buildRow = slots.headRow(located)
        while (buildRow >= 0) {
          TripleToIntLong.offerSlot(mapper(key, left(buildRow), probe), slot, sink)
          buildRow = slots.nextRow(buildRow)
        }
      row += 1
    }

  private def walkSlotSparse(
      slots: JoinSlots,
      columns: Sides,
      probeRows: Int,
      mapper: TripleToIntLong.TripleMap,
      slot: TripleToIntLong.IntLongSlot,
      sink: TripleToIntLong.LongSink,
  ): Unit =
    val probeKeys = columns.probeKeys.values
    val probeKeyWords = columns.probeKeys.validity.words
    val left = columns.buildValues.values
    val leftWords = columns.buildValues.validity.words
    val right = columns.probeValues.values
    val rightWords = columns.probeValues.validity.words
    val probeKeysDense = Validity.livePresent(columns.probeKeys.validity, probeRows)
    val leftDense = Validity.livePresent(columns.buildValues.validity, columns.buildValues.length)
    val rightDense = Validity.livePresent(columns.probeValues.validity, probeRows)
    var row = 0
    while (row < probeRows) {
      val keyOk = probeKeysDense || Validity.isSet(probeKeyWords, row)
      if (keyOk) then
        val key = probeKeys(row)
        val located = slots.locate(key)
        if (located >= 0) then
          val probeOk = rightDense || Validity.isSet(rightWords, row)
          var buildRow = slots.headRow(located)
          while (buildRow >= 0) {
            val buildOk = leftDense || Validity.isSet(leftWords, buildRow)
            if (!probeOk || !buildOk) then TripleToIntLong.rejectNull()
            TripleToIntLong.offerSlot(mapper(key, left(buildRow), right(row)), slot, sink)
            buildRow = slots.nextRow(buildRow)
          }
      row += 1
    }

  private object Drop extends TripleToIntLong.LongSink:
    def add(@unused key: Int, @unused value: Long): Unit = ()
