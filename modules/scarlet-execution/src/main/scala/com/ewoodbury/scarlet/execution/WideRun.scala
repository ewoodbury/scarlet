package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
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

  def batches(op: PhysicalOp): Seq[ColumnBatch] =
    run(op).map(_.batch)

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
    case PhysicalOp.WideCompute(child, expr, name) =>
      run(child).map { table =>
        val (fields, batch) = WideExpr.append(table.batch, table.fields, expr, name)
        Table(fields, batch)
      }
    case PhysicalOp.WideProject(child, spec) =>
      run(child).map { table =>
        val fields = spec.map { (ordinal, name) =>
          table.fields(ordinal).copy(name = name)
        }
        Table(fields, WideBatch.project(table.batch, spec.map(_._1)))
      }
    case PhysicalOp.WideHashJoin(left, right, leftKey, rightKey) =>
      Seq(join(left, right, leftKey, rightKey, broadcast = false))
    case PhysicalOp.WideBroadcastJoin(left, right, leftKey, rightKey) =>
      Seq(join(left, right, leftKey, rightKey, broadcast = true))
    case PhysicalOp.WideGroup(child, key, measure, key2) =>
      Seq(group(child, key, measure, key2))
    case other =>
      throw new IllegalStateException(s"wide run does not accept ${other.shape}")

  private def join(
      left: PhysicalOp,
      right: PhysicalOp,
      leftKey: Int,
      rightKey: Int,
      broadcast: Boolean,
  ): Table =
    val (leftFields, leftBatches) = side(left)
    val (rightFields, rightBatches) = side(right)
    val (fields, batch) =
      if (broadcast) then
        WideJoin.broadcast(leftBatches, rightBatches, leftFields, rightFields, leftKey, rightKey)
      else WideJoin.hash(leftBatches, rightBatches, leftFields, rightFields, leftKey, rightKey)
    Table(fields, batch)

  private def group(child: PhysicalOp, key: Int, measure: Int, key2: Option[Int]): Table =
    val tables = run(child)
    val fields = tables.headOption
      .map(_.fields)
      .orElse(WideLowering.fieldsOf(child))
      .getOrElse(
        throw new IllegalStateException(s"wide group child ${child.shape} has no schema"),
      )
    val (outFields, batch) =
      WideGroup.aggregate(tables.map(_.batch), fields, key, measure, key2)
    Table(outFields, batch)

  private def side(op: PhysicalOp): (Vector[ColumnField], Seq[ColumnBatch]) =
    val tables = run(op)
    val fields = tables.headOption
      .map(_.fields)
      .orElse(WideLowering.fieldsOf(op))
      .getOrElse(
        throw new IllegalStateException(s"wide join child ${op.shape} has no schema"),
      )
    (fields, tables.map(_.batch))
