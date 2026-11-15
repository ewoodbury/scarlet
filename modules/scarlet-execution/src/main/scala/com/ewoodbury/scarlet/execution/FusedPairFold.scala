package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{IntPairArrays, Partition}

/**
 * Folds a pair-filter count or sum over the source, without an encoded batch.
 *
 * Owned [[IntPairArrays]] have no nulls. Each partition is cut into the same ranges
 * [[com.ewoodbury.scarlet.columnar.Morsels]] would cut, and [[PairSourceFold]] reads those arrays.
 * A boxed partition is one pass: it is not random-access, a null component fails a step that reads
 * it and does not call that predicate, and the pass does not retain the columns. The long-pair
 * encode schedule is not used here. Int-pair encode was measured and is not a win when scheduled.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object FusedPairFold:

  def count(
      partitions: Seq[Partition[_]],
      steps: List[Selection.Step],
      batchSize: Int,
      parallelism: Int,
  ): Long =
    fold(partitions, batchSize, parallelism) { (keys, values, start, length) =>
      PairSourceFold.count(keys, values, start, length, steps)
    } { rows =>
      boxed(rows, steps, counting = true, measureOrdinal = 0)
    }

  def sum(
      partitions: Seq[Partition[_]],
      steps: List[Selection.Step],
      measureOrdinal: Int,
      batchSize: Int,
      parallelism: Int,
  ): Long =
    require(
      measureOrdinal == 0 || measureOrdinal == 1,
      s"sum ordinal $measureOrdinal is not a pair column",
    )
    fold(partitions, batchSize, parallelism) { (keys, values, start, length) =>
      PairSourceFold.sum(keys, values, start, length, steps, measureOrdinal)
    } { rows =>
      boxed(rows, steps, counting = false, measureOrdinal = measureOrdinal)
    }

  private enum Piece:
    case Owned(keys: Array[Int], values: Array[Int], start: Int, length: Int)
    case Boxed(rows: Iterable[Product])

  private def fold(
      partitions: Seq[Partition[_]],
      batchSize: Int,
      parallelism: Int,
  )(
      owned: (Array[Int], Array[Int], Int, Int) => Long,
  )(
      boxedRows: Iterable[Product] => Long,
  ): Long =
    val pieces = cuts(partitions, batchSize)
    if (pieces.isEmpty) then 0L
    else
      val partials = MorselScheduler.map(pieces, parallelism) { (_, piece) =>
        piece match
          case Piece.Owned(keys, values, start, length) => owned(keys, values, start, length)
          case Piece.Boxed(rows) => boxedRows(rows)
      }
      partials.foldLeft(0L)(_ + _)

  private def cuts(partitions: Seq[Partition[_]], batchSize: Int): Vector[Piece] =
    partitions.toVector.flatMap { partition =>
      partition.data match
        case owned: IntPairArrays =>
          ColumnBatches.cuts(owned.keys.length, batchSize).map { case (start, length) =>
            Piece.Owned(owned.keys, owned.values, start, length)
          }
        case other =>
          // Named erasure boundary. Classification already selected `(Int, Int)` elements.
          // `Product` keeps the read on `productElement`, so a null stays absent.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[Product]]
          Vector(Piece.Boxed(rows))
    }

  /**
   * One pass over boxed pairs. `counting` adds one per survivor. Otherwise `measureOrdinal` is
   * widened before the add. A null component fails a step that reads it and is not passed to that
   * predicate. A null measure adds nothing.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Var",
    ),
  )
  private def boxed(
      rows: Iterable[Product],
      steps: List[Selection.Step],
      counting: Boolean,
      measureOrdinal: Int,
  ): Long =
    require(steps.nonEmpty, "a filter stack needs a predicate")
    requireSteps(steps)
    val iterator = rows.iterator
    var total = 0L
    while (iterator.hasNext) {
      val pair = iterator.next()
      var keyPresent = false
      var key = 0
      var valuePresent = false
      var value = 0
      pair.productElement(0) match
        case null => keyPresent = false
        case found: Int =>
          keyPresent = true
          key = found
        case other =>
          reject(other, "key")
      pair.productElement(1) match
        case null => valuePresent = false
        case found: Int =>
          valuePresent = true
          value = found
        case other =>
          reject(other, "value")
      if (keeps(steps, keyPresent, key, valuePresent, value)) then
        if (counting) then total += 1L
        else
          val present = if (measureOrdinal == 0) then keyPresent else valuePresent
          if (present) then
            val stored = if (measureOrdinal == 0) then key else value
            total += stored.toLong
    }
    total

  @tailrec
  private def requireSteps(steps: List[Selection.Step]): Unit =
    steps match
      case Nil => ()
      case Selection.Step.Int32(column, _) :: rest =>
        require(column == 0 || column == 1, s"count ordinal $column is not a pair column")
        requireSteps(rest)
      case Selection.Step.IntPair(_) :: rest =>
        requireSteps(rest)
      case _ =>
        require(false, "count steps are int slots or int pairs")

  @tailrec
  private def keeps(
      steps: List[Selection.Step],
      keyPresent: Boolean,
      key: Int,
      valuePresent: Boolean,
      value: Int,
  ): Boolean =
    steps match
      case Nil => true
      case step :: rest =>
        if (keepsStep(step, keyPresent, key, valuePresent, value)) then
          keeps(rest, keyPresent, key, valuePresent, value)
        else false

  private def keepsStep(
      step: Selection.Step,
      keyPresent: Boolean,
      key: Int,
      valuePresent: Boolean,
      value: Int,
  ): Boolean =
    step match
      case Selection.Step.Int32(0, predicate) => keyPresent && predicate(key)
      case Selection.Step.Int32(1, predicate) => valuePresent && predicate(value)
      case Selection.Step.Int32(ordinal, _) =>
        require(false, s"count ordinal $ordinal is not a pair column")
        false
      case Selection.Step.IntPair(predicate) =>
        keyPresent && valuePresent && predicate(key, value)
      case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
          Selection.Step.Utf8(_, _) | Selection.Step.IntTriple(_, _) | Selection.Step.LongPair(_) |
          Selection.Step.StringPair(_) =>
        require(false, "count steps are int slots or int pairs")
        false

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
    ),
  )
  private def reject(found: Any, role: String): Nothing =
    val name = found.asInstanceOf[AnyRef].getClass.getName
    throw new IllegalArgumentException(s"pair component expected an Int $role, found $name")
