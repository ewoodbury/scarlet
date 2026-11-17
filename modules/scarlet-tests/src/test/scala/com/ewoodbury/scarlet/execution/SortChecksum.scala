package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnBatch, Int32Column}
import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Sort-action checksum: the sum of `key * 31 + value` over the sorted pairs.
 *
 * That is the cross-engine and phase-bench recipe. Columnar output is read from the key and value
 * arrays. A present batch does not call [[com.ewoodbury.scarlet.columnar.BatchCodec]], so the
 * checksum does not build a `Seq` of pairs or box the sum. A null component, or a plan that does
 * not run as int-pair columns, uses the collected pairs and the same arithmetic, which is what
 * `collect().map(...).sum` did. The row path does that too. Addition is commutative, including
 * overflow, so partition order does not change the result.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Equals"))
private[execution] object SortChecksum:

  def of(sorted: DistCollection[(Int, Int)]): Long =
    arrays(sorted).getOrElse(collected(sorted.collect()))

  /** Columnar sum, when every live key and value is present. `None` keeps the collected recipe. */
  private def arrays(sorted: DistCollection[(Int, Int)]): Option[Long] =
    if (!ScarletConf.get.columnarExecution) then None
    else if (ColumnarPlanner.partitioning(sorted.plan).isEmpty) then None
    else
      ColumnarPlanner.lower(sorted.plan) match
        case None => None
        case Some(op) =>
          sum(ColumnarInterpreter.interpret(sorted.plan, op))

  private def sum(columns: ColumnarInterpreter.Columns): Option[Long] =
    columns match
      case ColumnarInterpreter.Columns.Pairs(parts) => sumParts(parts)
      case _ => None

  private def sumParts(parts: Vector[ColumnBatch]): Option[Long] =
    var index = 0
    var sum = 0L
    var dense = true
    while (index < parts.length && dense) {
      add(parts(index)) match
        case Some(part) => sum += part
        case None => dense = false
      index += 1
    }
    if (dense) then Some(sum) else None

  private def add(batch: ColumnBatch): Option[Long] =
    (intColumn(batch, 0), intColumn(batch, 1)) match
      case (Some(keys), Some(values))
          if present(keys, batch.length) && present(values, batch.length) =>
        Some(sumArrays(keys.values, values.values, batch.length))
      case _ => None

  private def sumArrays(keys: Array[Int], values: Array[Int], rows: Int): Long =
    var row = 0
    var sum = 0L
    while (row < rows) {
      sum += keys(row).toLong * 31L + values(row).toLong
      row += 1
    }
    sum

  /**
   * Every row in `[0, live)` is present. Spare bits past `live` are ignored. This is the same word
   * test as `Validity.livePresent`, which the harness cannot call.
   */
  private def present(column: Int32Column, live: Int): Boolean =
    if (live == 0) then true
    else
      val words = column.validity.words
      val full = live >>> 6
      var index = 0
      var dense = true
      while (index < full && dense) {
        if (words(index) != -1L) then dense = false
        index += 1
      }
      if (!dense) then false
      else
        val tail = live & 63
        if (tail == 0) then true
        else
          val mask = (1L << tail) - 1L
          (words(full) & mask) == mask

  private def intColumn(batch: ColumnBatch, ordinal: Int): Option[Int32Column] =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => Some(column)
      case _ => None

  /** Same sum as `rows.map { case (key, value) => key * 31L + value }.sum`. */
  private def collected(rows: Iterable[(Int, Int)]): Long =
    val iterator = rows.iterator
    var sum = 0L
    while (iterator.hasNext) {
      val pair = iterator.next()
      sum += pair._1.toLong * 31L + pair._2.toLong
    }
    sum
