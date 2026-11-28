package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.{Cell, ColumnField, Partition, Plan, WideRow}

/**
 * Drops columns of a wide group on the row path. The group runs in [[WideGroupRows]]. A project
 * over a scan stays a stage.
 */
private[execution] object WideProjectRows:

  def partitions(group: Plan.WideGroupOp, spec: Vector[(Int, String)]): Seq[Partition[WideRow]] =
    require(spec.nonEmpty, "project needs a column")
    val names = spec.map(_._2)
    require(names.forall(_.nonEmpty), "column name must not be empty")
    require(names.distinct.size == names.size, "column names must be unique")
    val ordinals = spec.map(_._1)
    require(ordinals.distinct.size == ordinals.size, "project ordinals must be unique")
    WideGroupRows.partitions(group).map { part =>
      Partition(part.data.map(row => project(row, spec)))
    }

  private def project(row: WideRow, spec: Vector[(Int, String)]): WideRow =
    WideRow(
      spec.map { (ordinal, name) => fieldAt(row, ordinal).copy(name = name) },
      spec.map { (ordinal, _) => cellAt(row, ordinal) },
    )

  private def fieldAt(row: WideRow, ordinal: Int): ColumnField =
    row.fields.lift(ordinal).getOrElse {
      throw new IllegalArgumentException(s"column $ordinal outside width ${row.fields.size}")
    }

  private def cellAt(row: WideRow, ordinal: Int): Cell =
    row.cells.lift(ordinal).getOrElse {
      throw new IllegalArgumentException(s"column $ordinal outside width ${row.cells.size}")
    }
