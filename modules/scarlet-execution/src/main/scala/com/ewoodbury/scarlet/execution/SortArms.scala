package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{ColumnBatch, IntTopN, Selected}
import com.ewoodbury.scarlet.core.Plan

/**
 * Int-pair sort by value descending then key ascending.
 *
 * [[sort]] is the full order for collect. [[take]] is a bounded heap of `n` rows. An owned
 * int-pair scan, with or without an int-slot filter stack, walks those arrays in
 * [[OwnedValueTake]] and does not encode them. One int-pair project, one [[PhysicalOp.MapKey]],
 * one key or value filter immediately above that map, or one int [[PhysicalOp.MapValue]], on a
 * scan or a filter stack, has no survivor index. [[FilterChain.selected]] returns `None`,
 * interpret publishes those pairs, and [[IntTopN]] orders the batches. One key or value filter
 * immediately above that project keeps the index when it names the projected rows, and [[IntTopN]]
 * orders those rows. A null input key stays null, is not passed to the key function, and orders as
 * zero. A null value stays null, is not passed to `mapValues`, and orders as zero. Any other child
 * selects, then heaps, and uses the full sort when `n` covers every live row. A null value or key
 * orders as zero and stays null. Ties keep input order.
 */
private[execution] object SortArms:

  def sort(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): ColumnarInterpreter.Columns =
    ColumnarInterpreter.Columns.Pairs(sorted(source, child, ordinals))

  def take(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
      n: Int,
  ): Vector[ColumnBatch] =
    OwnedValueTake.take(source, child, ordinals, n) match
      case Some(batch) => Vector(batch)
      case None => Vector(prefix(source, child, ordinals, n))

  private def sorted(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    survivors(source, child, ordinals) match
      case Left(batches) => IntTopN.sortBatches(batches)
      case Right(selected) => IntTopN.sortSelected(selected)

  private def prefix(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
      n: Int,
  ): ColumnBatch =
    survivors(source, child, ordinals) match
      case Left(batches) => IntTopN.takeBatches(batches, n)
      case Right(selected) => IntTopN.takeSelected(selected, n)

  private def survivors(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Either[Vector[ColumnBatch], Vector[Selected]] =
    val selected = FilterChain.selected(
      source,
      child,
      selectedPartition = None,
      preserveAbsence = true,
      ordinals,
    )
    selected match
      case Some(parts) =>
        Right[Vector[ColumnBatch], Vector[Selected]](parts)
      case None =>
        Left[Vector[ColumnBatch], Vector[Selected]](
          pairBatches(
            ColumnarInterpreter.interpretAt(
              source,
              child,
              selectedPartition = None,
              preserveAbsence = true,
              ordinals,
            ),
          ),
        )

  private def pairBatches(columns: ColumnarInterpreter.Columns): Vector[ColumnBatch] =
    columns match
      case ColumnarInterpreter.Columns.Pairs(parts) => parts
      case other =>
        throw new IllegalStateException(s"int value-desc sort expected int pairs, got $other")
