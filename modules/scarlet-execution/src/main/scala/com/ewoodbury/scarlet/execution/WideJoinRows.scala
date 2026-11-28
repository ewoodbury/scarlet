package com.ewoodbury.scarlet.execution

import scala.collection.mutable

import com.ewoodbury.scarlet.core.*

/**
 * In-memory inner join of wide rows. This is the row oracle for [[WideJoin]].
 *
 * A null key is dropped. A null payload stays. Output is every left column, then every right
 * column except the key. Hash and broadcast name the same multiset: the build is grouped by key,
 * not by shuffle bucket. The caller names the strategy on the plan. A key that is not int or long
 * throws, so a refused columnar plan does not invent a result. A left outer join keeps each
 * unmatched left row once. A missing partner is null. A stored zero stays present.
 */
private[execution] object WideJoinRows:

  def partitions(join: Plan.WideJoinOp): Seq[Partition[WideRow]] =
    val (leftFields, leftRows) = materialize(join.left)
    val (rightFields, rightRows) = materialize(join.right)
    val leftKind = keyKind(leftFields, join.leftKey, "left")
    val rightKind = keyKind(rightFields, join.rightKey, "right")
    if (!joinable(leftKind, rightKind)) then
      throw new IllegalArgumentException(s"join keys are $leftKind and $rightKind")
    val fields = leftFields ++ rightFields.zipWithIndex.collect {
      case (field, ordinal) if ordinal != join.rightKey => field
    }
    Seq(Partition(matchRows(leftRows, rightRows, join.leftKey, join.rightKey, fields)))

  def leftPartitions(join: Plan.WideLeftJoinOp): Seq[Partition[WideRow]] =
    val (leftFields, leftRows) = materialize(join.left)
    val (rightFields, rightRows) = materialize(join.right)
    val leftKind = keyKind(leftFields, join.leftKey, "left")
    val rightKind = keyKind(rightFields, join.rightKey, "right")
    if (!joinable(leftKind, rightKind)) then
      throw new IllegalArgumentException(s"join keys are $leftKind and $rightKind")
    val fields = leftFields ++ rightFields.zipWithIndex.collect {
      case (field, ordinal) if ordinal != join.rightKey => field
    }
    Seq(Partition(keepLeft(leftRows, rightRows, join.leftKey, join.rightKey, fields)))

  private def materialize(plan: Plan[WideRow]): (Vector[ColumnField], Vector[WideRow]) =
    val parts = ExecutionService.get.executePartitions(plan)
    val fromColumns = parts.iterator.map(_.data).collectFirst { case columns: WideColumns =>
      columns.fields
    }
    val rows = parts.flatMap(_.data).toVector
    val fields = fromColumns
      .orElse(WideFields.known(plan))
      .orElse(rows.headOption.map(_.fields))
      .getOrElse(throw new IllegalArgumentException("wide join has no schema"))
    (fields, rows)

  private def matchRows(
      leftRows: Vector[WideRow],
      rightRows: Vector[WideRow],
      leftKey: Int,
      rightKey: Int,
      fields: Vector[ColumnField],
  ): Vector[WideRow] =
    val build = mutable.HashMap.empty[Cell, List[Vector[Cell]]]
    rightRows.foreach { row =>
      cellAt(row, rightKey) match
        case Cell.Null => ()
        case key =>
          val payload = row.cells.zipWithIndex.collect {
            case (cell, ordinal) if ordinal != rightKey => cell
          }
          build.updateWith(key) {
            case Some(hits) => Some(payload :: hits)
            case None => Some(payload :: Nil)
          }
    }
    val matched = Vector.newBuilder[WideRow]
    leftRows.foreach { row =>
      cellAt(row, leftKey) match
        case Cell.Null => ()
        case key =>
          build.get(key).foreach { hits =>
            hits.foreach { payload =>
              matched += WideRow(fields, row.cells ++ payload)
            }
          }
    }
    matched.result()

  private def keepLeft(
      leftRows: Vector[WideRow],
      rightRows: Vector[WideRow],
      leftKey: Int,
      rightKey: Int,
      fields: Vector[ColumnField],
  ): Vector[WideRow] =
    val build = mutable.HashMap.empty[Cell, List[Vector[Cell]]]
    rightRows.foreach { row =>
      cellAt(row, rightKey) match
        case Cell.Null => ()
        case key =>
          val payload = row.cells.zipWithIndex.collect {
            case (cell, ordinal) if ordinal != rightKey => cell
          }
          build.updateWith(key) {
            case Some(hits) => Some(payload :: hits)
            case None => Some(payload :: Nil)
          }
    }
    val matched = Vector.newBuilder[WideRow]
    leftRows.foreach { row =>
      cellAt(row, leftKey) match
        case Cell.Null => ()
        case key =>
          build.get(key) match
            case Some(hits) =>
              hits.foreach { payload =>
                matched += WideRow(fields, row.cells ++ payload)
              }
            case None =>
              val missing = Vector.fill(fields.size - row.cells.size)(Cell.Null)
              matched += WideRow(fields, row.cells ++ missing)
    }
    matched.result()

  private def cellAt(row: WideRow, ordinal: Int): Cell =
    if (ordinal >= 0 && ordinal < row.cells.size) then row.cells(ordinal)
    else throw new IllegalArgumentException(s"column $ordinal outside width ${row.cells.size}")

  private def keyKind(fields: Vector[ColumnField], ordinal: Int, side: String): ColumnKind =
    if (ordinal >= 0 && ordinal < fields.size) then fields(ordinal).kind
    else throw new IllegalArgumentException(s"$side key $ordinal is outside the row")

  private def joinable(left: ColumnKind, right: ColumnKind): Boolean = (left, right) match
    case (ColumnKind.Int32, ColumnKind.Int32) => true
    case (ColumnKind.Int64, ColumnKind.Int64) => true
    case _ => false
