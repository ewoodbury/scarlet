package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{ColumnBatch, WideBatch}
import com.ewoodbury.scarlet.core.{ColumnField, Partition, WideColumns, WideRow}

/**
 * Runs a wide scan, filter, and project without the element interpreter.
 *
 * The physical tree is encoded from owned [[WideColumns]], filtered and projected as batches, and
 * decoded to [[WideRow]] only at the boundary. A compare the lowering refused never reaches this
 * object.
 */
private[execution] object WideRun:

  def partitions(op: PhysicalOp): Seq[Partition[WideRow]] =
    run(op).map { table =>
      Partition(WideBatch.decode(table.batch, table.fields))
    }

  def take(op: PhysicalOp, n: Int): Option[Seq[WideRow]] =
    val rows = run(op).iterator.flatMap { table =>
      WideBatch.decode(table.batch, table.fields).iterator
    }
    Some(rows.take(n).toSeq)

  def rowCount(op: PhysicalOp): Long =
    run(op).foldLeft(0L)((total, table) => total + table.batch.length.toLong)

  private final case class Table(fields: Vector[ColumnField], batch: ColumnBatch)

  private def run(op: PhysicalOp): Seq[Table] = op match
    case PhysicalOp.Scan(PhysicalOp.Kind.Columns, partitions) =>
      partitions.map { part =>
        part.data match
          case columns: WideColumns => Table(columns.fields, WideBatch.encode(columns))
          case other =>
            throw new IllegalStateException(
              s"wide scan expected WideColumns, found ${other.getClass.getName}",
            )
      }
    case PhysicalOp.WideFilter(child, ordinal, compare) =>
      run(child).map { table =>
        Table(table.fields, WideBatch.filter(table.batch, ordinal, compare))
      }
    case PhysicalOp.WideProject(child, spec) =>
      run(child).map { table =>
        val fields = spec.map { (ordinal, name) =>
          table.fields(ordinal).copy(name = name)
        }
        Table(fields, WideBatch.project(table.batch, spec.map(_._1)))
      }
    case other =>
      throw new IllegalStateException(s"wide run does not accept ${other.shape}")
