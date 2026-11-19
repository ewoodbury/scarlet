package com.ewoodbury.scarlet.columnar

/**
 * `reduceByKey` after one int slot above `mapIntKey`.
 *
 * The input morsels are the map's source. They are already filtered. Each present key is passed to
 * `mapKey` once, then the slot sees that mapped key or the original value. A null input key is not
 * passed to `mapKey` or to a key predicate. A value predicate still sees that row, including a
 * null key, and is not called on a null value. A null mapped key is the null input key: it is
 * dropped and does not create a group. A null value is dropped after `mapKey` runs and does not
 * create a group. The pair batch is never published, and [[HashReduce.byKey]] does not see it.
 * Neither function is inspected.
 *
 * Partials are one reduce per morsel, merged per input batch, then exchanged like
 * [[MappedKeyReduce]]. Spill stays on the post-exchange bucket.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object MappedAboveReduce:

  def reduce(
      batches: Vector[ColumnBatch],
      mapKey: Int => Int,
      above: Selection.Step,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: HashReduce.IntBinOp,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val (column, keep) = slot(above)
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
    else
      val groups = batches.map(batch => Morsels.of(batch, batchSize))
      val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
        partial(morsel, mapKey, column, keep, op)
      }
      val combined = PartialCombine.mergeMorsels(
        groups.map(_.length),
        partials,
        parallelism,
        batch => HashReduce.int32(batch, keyOrdinal = 0, valueOrdinal = 1, op),
      )
      HashReduce.reduceExchanged(combined, numPartitions, parallelism, batchSize, op)

  /**
   * One int key or value. The predicate is the function the planner stored, not a second wrapper.
   */
  private def slot(step: Selection.Step): (Int, Int => Boolean) = step match
    case Selection.Step.Int32(column, predicate) if column == 0 || column == 1 =>
      (column, captured(predicate))
    case Selection.Step.Int32(column, _) =>
      throw new IllegalStateException(s"mapped slot must be a key or a value, got $column")
    case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
        Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.LongPair(_) |
        Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
      throw new IllegalStateException("slot above a mapped key is an int key or value")

  private def captured(predicate: ColumnKernel.Int32Predicate): Int => Boolean =
    predicate match
      case direct: Selection.Int32Function => direct.call
      case other =>
        val fallback = other
        value => fallback(value)

  private def partial(
      morsel: Morsel,
      mapKey: Int => Int,
      column: Int,
      keep: Int => Boolean,
      op: HashReduce.IntBinOp,
  ): ColumnBatch =
    HashReduce.partialPairs(morsel.length, op) { sink =>
      visit(morsel.batch, morsel.start, morsel.length, mapKey, column, keep, sink)
      ColumnMetrics.fused()
    }

  /**
   * Present keys are mapped, then the slot runs. A null key is not passed to `mapKey` or a key
   * predicate. A value predicate still sees that row. A null value is not passed to a value
   * predicate and is not inserted. An empty range does not call `mapKey`.
   */
  private def visit(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      mapKey: Int => Int,
      column: Int,
      keep: Int => Boolean,
      sink: IntPairProject.IntPairSink,
  ): Unit =
    require(
      batch.columns.length == 2,
      s"mapped key reduce expected 2 columns, got ${batch.columns.length}",
    )
    val keys = ColumnHash.int32At(batch, 0)
    val values = ColumnHash.int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    Ranges.check(start, length, keys.length)
    if (length == 0) then ()
    else if (
        Validity.rangePresent(keys.validity, start, length) &&
        Validity.rangePresent(values.validity, start, length)
      )
    then
      if (column == 0) then visitDenseKey(keys, values, start, length, mapKey, keep, sink)
      else visitDenseValue(keys, values, start, length, mapKey, keep, sink)
    else if (column == 0) then visitSparseKey(keys, values, start, length, mapKey, keep, sink)
    else visitSparseValue(keys, values, start, length, mapKey, keep, sink)

  private def visitDenseKey(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      mapKey: Int => Int,
      keep: Int => Boolean,
      sink: IntPairProject.IntPairSink,
  ): Unit =
    val keyArr = keys.values
    val valueArr = values.values
    val end = start + length
    var row = start
    while (row < end) {
      val mapped = mapKey(keyArr(row))
      if (keep(mapped)) then sink.add(mapped, valueArr(row))
      row += 1
    }

  private def visitDenseValue(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      mapKey: Int => Int,
      keep: Int => Boolean,
      sink: IntPairProject.IntPairSink,
  ): Unit =
    val keyArr = keys.values
    val valueArr = values.values
    val end = start + length
    var row = start
    while (row < end) {
      val mapped = mapKey(keyArr(row))
      if (keep(valueArr(row))) then sink.add(mapped, valueArr(row))
      row += 1
    }

  private def visitSparseKey(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      mapKey: Int => Int,
      keep: Int => Boolean,
      sink: IntPairProject.IntPairSink,
  ): Unit =
    val keyArr = keys.values
    val valueArr = values.values
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    val end = start + length
    var row = start
    while (row < end) {
      if (Validity.isSet(keyWords, row)) then
        val mapped = mapKey(keyArr(row))
        if (keep(mapped) && Validity.isSet(valueWords, row)) then sink.add(mapped, valueArr(row))
      row += 1
    }

  private def visitSparseValue(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      mapKey: Int => Int,
      keep: Int => Boolean,
      sink: IntPairProject.IntPairSink,
  ): Unit =
    val keyArr = keys.values
    val valueArr = values.values
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    val end = start + length
    var row = start
    while (row < end) {
      val valueOn = Validity.isSet(valueWords, row)
      if (Validity.isSet(keyWords, row)) then
        val mapped = mapKey(keyArr(row))
        if (valueOn && keep(valueArr(row))) then sink.add(mapped, valueArr(row))
      else if (valueOn) then
        val _ = keep(valueArr(row))
      row += 1
    }
