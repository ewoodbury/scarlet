package com.ewoodbury.scarlet.columnar

/**
 * `reduceByKey` after an int key map.
 *
 * The input morsels are the map's source. They are already filtered. Each present key is passed to
 * `mapKey` once. A null key stays null and is not passed. `mapKey` returns `Int`, so a present
 * source key has a present mapped key. That key and the original value are inserted into the
 * [[HashReduce]] table. A null source key is the null mapped key: it is dropped and does not
 * create a group. A null value is dropped after `mapKey` runs and does not create a group. The
 * pair batch is never published, and [[HashReduce.byKey]] does not see it. The function is not
 * inspected.
 *
 * Partials are one reduce per morsel, merged per input batch, then exchanged like [[HashReduce]].
 * Spill stays on the post-exchange bucket.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object MappedKeyReduce:

  def reduce(
      batches: Vector[ColumnBatch],
      mapKey: Int => Int,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: HashReduce.IntBinOp,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
    else
      val groups = batches.map(batch => Morsels.of(batch, batchSize))
      val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
        partial(morsel, mapKey, op)
      }
      val combined = PartialCombine.mergeMorsels(
        groups.map(_.length),
        partials,
        parallelism,
        batch => HashReduce.int32(batch, keyOrdinal = 0, valueOrdinal = 1, op),
      )
      HashReduce.reduceExchanged(combined, numPartitions, parallelism, batchSize, op)

  private def partial(
      morsel: Morsel,
      mapKey: Int => Int,
      op: HashReduce.IntBinOp,
  ): ColumnBatch =
    HashReduce.partialPairs(morsel.length, op) { sink =>
      visit(morsel.batch, morsel.start, morsel.length, mapKey, sink)
      ColumnMetrics.fused()
    }

  /**
   * Present keys are mapped. A null key is not passed to `mapKey` and is not inserted. A null
   * value is dropped and does not create a group. An empty range does not call `mapKey`.
   */
  private def visit(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      mapKey: Int => Int,
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
    then visitDense(keys, values, start, length, mapKey, sink)
    else visitSparse(keys, values, start, length, mapKey, sink)

  private def visitDense(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      mapKey: Int => Int,
      sink: IntPairProject.IntPairSink,
  ): Unit =
    val keyArr = keys.values
    val valueArr = values.values
    val end = start + length
    var row = start
    while (row < end) {
      sink.add(mapKey(keyArr(row)), valueArr(row))
      row += 1
    }

  private def visitSparse(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      mapKey: Int => Int,
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
        if (Validity.isSet(valueWords, row)) then sink.add(mapped, valueArr(row))
      row += 1
    }
