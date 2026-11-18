package com.ewoodbury.scarlet.columnar

/**
 * `reduceByKey` over a `mapIntKey`.
 *
 * The map's input morsels are already filtered. `mapKey` runs once on each present key and is not
 * inspected. A null key stays null and is not passed in. That null key is dropped. A null value is
 * dropped and does not create a group, which is [[HashReduce]]'s rule. The mapped pair batch is
 * never published, and [[HashReduce.byKey]] does not see it.
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
   * value is not inserted. `mapKey` still runs for a present key whose value is null, then the row
   * is dropped.
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
      s"mapIntKey reduce expected 2 columns, got ${batch.columns.length}",
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
