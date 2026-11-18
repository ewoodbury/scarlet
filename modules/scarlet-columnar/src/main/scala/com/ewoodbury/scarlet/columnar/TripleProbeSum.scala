package com.ewoodbury.scarlet.columnar

import scala.annotation.unused

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
 * name a bad cell. It does not publish a sum.
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

  private object Drop extends TripleToIntLong.LongSink:
    def add(@unused key: Int, @unused value: Long): Unit = ()
