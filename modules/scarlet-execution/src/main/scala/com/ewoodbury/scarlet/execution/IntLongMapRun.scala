package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{ColumnBatch, IntLongMap}
import com.ewoodbury.scarlet.core.Partition

import ColumnarInterpreter.{Columns, onEachMorsel, pairsOf}

/**
 * Runs [[PhysicalOp.IntLongMapValue]] and decodes its `(Int32, Int64)` batches.
 *
 * The cast is the erasure boundary. [[FunctionWitness.declaredIntToLong]] already required
 * `apply$mcJI$sp`. This does not classify the function by calling it. Decoding goes through
 * [[IntLongMap.decode]], not the int-pair decoder.
 */
private[execution] object IntLongMapRun:

  /**
   * The widening, a filter of it, a `Long => Long` map of it, or the non-adding int-long reduce.
   * Not an int pair. The reduce is not part of [[IntLongParents.widened]].
   */
  def decodes(op: PhysicalOp): Boolean =
    IntLongParents.widened(op) || (op match
      case _: PhysicalOp.IntLongHashAggregate | _: PhysicalOp.SortByIntLongKey |
          _: PhysicalOp.SortByIntLongValueDesc =>
        true
      case _ => false)

  def columns(input: Columns, mapper: Any): Columns =
    val fn = longValue(mapper)
    Columns.Pairs(
      onEachMorsel(pairsOf(input)) { morsel =>
        IntLongMap.widen(morsel.batch, morsel.start, morsel.length, fn)
      },
    )

  def partitions(columns: Columns): Seq[Partition[_]] = columns match
    case Columns.Pairs(parts) =>
      parts.map(batch => Partition(decode(batch, batch.length)))
    case _ =>
      throw new IllegalStateException("int-long mapValues expected pair batches")

  def prefix(columns: Columns, limit: Int): Seq[Any] =
    if (limit <= 0) then Seq.empty[Any]
    else
      columns match
        case Columns.Pairs(parts) =>
          ColumnarPlanner.takeChunks(
            limit,
            parts.iterator.map { batch => (remaining: Int) =>
              decode(batch, remaining)
            },
          )
        case _ =>
          throw new IllegalStateException("int-long mapValues expected pair batches")

  /** Named erasure boundary. Lowering checked `apply$mcJI$sp`, so this cast is safe. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def longValue(mapper: Any): Int => Long =
    mapper.asInstanceOf[Int => Long]

  private def decode(batch: ColumnBatch, maxRows: Int): Seq[(Int, Long)] =
    IntLongMap.decode(batch, maxRows)
