package com.ewoodbury.scarlet.execution

import scala.collection.mutable

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{IntPairArrays, Plan}

/**
 * Grouped inputs whose morsels must not take the direct index.
 *
 * `sparse` puts 1024 groups across `0` until `300000`. `shifting` uses that same spread and adds
 * 7 on every odd morsel, so the minimum changes every morsel and the span stays wide. Both hash.
 * The check compares one partial with first-seen order. A direct index would sort the keys.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Var",
    "org.wartremover.warts.MutableDataStructures",
  )
)
private[execution] object GroupedBenchCases:

  val sparseSpan: Int = 300000
  val shiftOrigin: Int = 7
  private val groups = 1024

  final case class Fold(hash: Long, total: Long)

  final case class Input(source: DistCollection[(Int, Int)], hash: Long, total: Long)

  def sparse(rows: Int, partitions: Int, batchSize: Int): Input =
    owned(fill(rows, partitions, batchSize, shifting = false), rows, partitions)

  def shifting(rows: Int, partitions: Int, batchSize: Int): Input =
    owned(fill(rows, partitions, batchSize, shifting = true), rows, partitions)

  /** Fails the run unless both patterns hash. Prints the path so the bench log shows it. */
  def requireHashFallback(sparseInput: Input, shiftingInput: Input, batchSize: Int): Unit =
    requireOrder(sparseInput, batchSize, morsel = 0, origin = 0)
    requireOrder(shiftingInput, batchSize, morsel = 0, origin = 0)
    requireOrder(shiftingInput, batchSize, morsel = 1, origin = shiftOrigin)
    System.err.println("grouped-sparse path=hash")
    System.err.println("grouped-shifting path=hash")

  def sourceFold(source: DistCollection[(Int, Int)]): Fold =
    val sums = mutable.HashMap.empty[Int, Long]
    source.plan match
      case Plan.Source(partitions, _) =>
        partitions.foreach { partition =>
          partition.data match
            case pairs: IntPairArrays => addPairs(pairs.keys, pairs.values, sums)
            case _ =>
              throw new IllegalStateException("grouped source was not an owned pair array")
        }
      case _ => throw new IllegalStateException("grouped plan was not a source")
    finish(sums)

  def columnarFold(source: DistCollection[(Int, Int)]): Fold =
    source.plan match
      case sourcePlan: Plan.Source[_] =>
        val encoded = ColumnarInterpreter.encodeGroupedPairs(sourcePlan.partitions)
        val summed = ColumnPipeline.sumByKey(
          encoded,
          keyOrdinal = 0,
          valueOrdinal = 1,
          numPartitions = ColumnarPlanner.shuffleWidth,
          parallelism = ColumnarPlanner.parallelism,
          batchSize = ColumnarPlanner.batchSize,
          steps = Vector.empty[PairStep],
        )
        val sums = mutable.HashMap.empty[Int, Long]
        summed.foreach(batch => addBatch(batch, sums))
        finish(sums)
      case _ => throw new IllegalStateException("grouped plan was not a source")

  private def owned(keys: Array[Int], rows: Int, partitions: Int): Input =
    val values = Array.tabulate(rows)(row => row)
    val source = DistCollection.ofIntPairs(keys, values, partitions)
    val fold = sourceFold(source)
    val closed = rows.toLong * (rows.toLong - 1L) / 2L
    require(fold.total == closed, "spread groups drifted from the closed sum")
    Input(source, fold.hash, fold.total)

  private def fill(rows: Int, partitions: Int, batchSize: Int, shifting: Boolean): Array[Int] =
    val keys = new Array[Int](rows)
    val partRows = rows / partitions
    require(partRows * partitions == rows, "grouped rows do not split evenly")
    require(batchSize > 0 && batchSize <= partRows, s"batch $batchSize does not fit a partition")
    var part = 0
    while (part < partitions) {
      val origin = part * partRows
      var start = 0
      var morsel = 0
      while (start < partRows) {
        val length = math.min(batchSize, partRows - start)
        val base = if (shifting && (morsel & 1) == 1) then shiftOrigin else 0
        var row = 0
        while (row < length) {
          val slot = groups - 1 - (row % groups)
          keys(origin + start + row) = base + spread(slot)
          row += 1
        }
        morsel += 1
        start += length
      }
      part += 1
    }
    keys

  private def spread(slot: Int): Int =
    ((slot.toLong * sparseSpan.toLong) / (groups - 1).toLong).toInt

  private def requireOrder(input: Input, batchSize: Int, morsel: Int, origin: Int): Unit =
    val stored = partitionKeys(input.source)
    val start = morsel * batchSize
    require(
      start + batchSize <= stored.length,
      s"morsel $morsel does not fit the first partition",
    )
    val seen = firstSeen(stored, start, batchSize)
    val got = partialKeys(stored, start, batchSize)
    require(same(got, seen), s"morsel $morsel lost first-seen order")
    require(!same(got, sorted(seen)), s"morsel $morsel was sorted, so the dense path ran")
    require(got(0) == origin + sparseSpan, s"morsel $morsel started at ${got(0)}")

  /** One morsel, so the check does not aggregate the whole bench input. */
  private def partialKeys(keys: Array[Int], start: Int, length: Int): Array[Int] =
    val slice = java.util.Arrays.copyOfRange(keys, start, start + length)
    val values = new Array[Int](length)
    val source = DistCollection.ofIntPairs(slice, values, 1)
    source.plan match
      case sourcePlan: Plan.Source[_] =>
        val encoded = ColumnarInterpreter.encodeGroupedPairs(sourcePlan.partitions)
        val (_, morsels) = GroupedSumKernel.partialMorsels(
          encoded,
          keyOrdinal = 0,
          valueOrdinal = 1,
          parallelism = 1,
          batchSize = length,
          steps = Vector.empty[PairStep],
        )
        morsels match
          case Vector(batch) => keysOf(batch)
          case other =>
            throw new IllegalStateException(s"expected one partial, got ${other.length}")
      case _ => throw new IllegalStateException("grouped plan was not a source")

  private def partitionKeys(source: DistCollection[(Int, Int)]): Array[Int] =
    source.plan match
      case Plan.Source(partitions, _) =>
        partitions.headOption match
          case Some(partition) =>
            partition.data match
              case pairs: IntPairArrays => pairs.keys
              case _ =>
                throw new IllegalStateException("grouped source was not an owned pair array")
          case None => throw new IllegalStateException("grouped source had no partition")
      case _ => throw new IllegalStateException("grouped plan was not a source")

  private def firstSeen(keys: Array[Int], start: Int, length: Int): Array[Int] =
    val ordered = mutable.LinkedHashSet.empty[Int]
    var index = 0
    while (index < length) {
      ordered.add(keys(start + index))
      index += 1
    }
    ordered.toArray

  private def sorted(keys: Array[Int]): Array[Int] =
    val copy = keys.clone()
    java.util.Arrays.sort(copy)
    copy

  private def same(left: Array[Int], right: Array[Int]): Boolean =
    java.util.Arrays.equals(left, right)

  private def keysOf(batch: ColumnBatch): Array[Int] =
    batch.columns.headOption match
      case Some(column: Int32Column) =>
        java.util.Arrays.copyOf(column.values, batch.length)
      case _ => throw new IllegalStateException("grouped key column was not int32")

  private def addPairs(keys: Array[Int], values: Array[Int], sums: mutable.HashMap[Int, Long]): Unit =
    var index = 0
    while (index < keys.length) {
      addGroup(keys(index), values(index).toLong, sums)
      index += 1
    }

  private def addBatch(batch: ColumnBatch, sums: mutable.HashMap[Int, Long]): Unit =
    if (batch.length != 0) then
      val keys = keysOf(batch)
      val values = longSums(batch)
      var row = 0
      while (row < batch.length) {
        require(values.isValid(row), "grouped checksum saw a null sum")
        addGroup(keys(row), values.values(row), sums)
        row += 1
      }

  private def addGroup(key: Int, value: Long, sums: mutable.HashMap[Int, Long]): Unit =
    sums.update(key, sums.getOrElse(key, 0L) + value)

  private def finish(sums: mutable.HashMap[Int, Long]): Fold =
    val keys = sums.keys.toArray
    java.util.Arrays.sort(keys)
    var index = 0
    var hash = 0L
    var total = 0L
    while (index < keys.length) {
      val key = keys(index)
      val sum = sums(key)
      total += sum
      hash += (key.toLong * 31L) ^ sum
      index += 1
    }
    Fold(hash, total)

  private def longSums(batch: ColumnBatch): Int64Column =
    batch.columns.lift(1) match
      case Some(column: Int64Column) => column
      case _ => throw new IllegalStateException("grouped sum column was not int64")
