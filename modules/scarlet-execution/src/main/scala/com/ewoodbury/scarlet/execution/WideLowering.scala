package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.{ColumnCompare, ColumnField, WideColumns, WideRow}

/**
 * Lowers a wide-row filter or project when the child is already a wide scan, filter, or project
 * and the compare is one the closed set allows. A refused compare returns [[None]] so the action
 * reruns on rows.
 */
private[execution] object WideLowering:

  def filter(
      source: PlanNode,
      ordinal: Int,
      compare: ColumnCompare,
      lowerChild: com.ewoodbury.scarlet.core.Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerChild(source).flatMap { child =>
      column(child, ordinal) match
        case Some(field) if compare.allows(field.kind) =>
          Some(PhysicalOp.WideFilter(child, ordinal, compare))
        case _ =>
          LoweringTrace.note("WideFilterOp", child.kind, "WideLowering")
          None
    }

  def compute(
      source: com.ewoodbury.scarlet.core.Plan[WideRow],
      expr: com.ewoodbury.scarlet.core.ColumnExpr,
      name: String,
      lowerChild: com.ewoodbury.scarlet.core.Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    if (name.isEmpty) then None
    else
      lowerChild(source).flatMap { child =>
        fieldsOf(child) match
          case Some(fields)
              if com.ewoodbury.scarlet.core.ColumnExpr.resultKind(expr, fields).isDefined &&
                !fields.exists(field => field.name.equals(name)) =>
            Some(PhysicalOp.WideCompute(child, expr, name))
          case Some(_) =>
            LoweringTrace.note("WideComputeOp", child.kind, "WideLowering")
            None
          case None =>
            None
      }

  def join(
      left: PlanNode,
      right: PlanNode,
      leftKey: Int,
      rightKey: Int,
      broadcast: Option[Boolean],
      lowerChild: com.ewoodbury.scarlet.core.Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    joined(left, right, leftKey, rightKey, broadcast, lowerChild, outer = false)

  def leftJoin(
      left: PlanNode,
      right: PlanNode,
      leftKey: Int,
      rightKey: Int,
      broadcast: Option[Boolean],
      lowerChild: com.ewoodbury.scarlet.core.Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    joined(left, right, leftKey, rightKey, broadcast, lowerChild, outer = true)

  private def joined(
      left: PlanNode,
      right: PlanNode,
      leftKey: Int,
      rightKey: Int,
      broadcast: Option[Boolean],
      lowerChild: com.ewoodbury.scarlet.core.Plan[_] => Option[PhysicalOp],
      outer: Boolean,
  ): Option[PhysicalOp] =
    (lowerChild(left), lowerChild(right)) match
      case (Some(leftOp), Some(rightOp)) =>
        (fieldsOf(leftOp), fieldsOf(rightOp)) match
          case (Some(leftFields), Some(rightFields))
              if keysMatch(leftFields, rightFields, leftKey, rightKey) =>
            val useBroadcast = broadcast.getOrElse(JoinCost.preferBroadcast(rightOp))
            val chosen =
              if (outer && useBroadcast) then
                PhysicalOp.WideBroadcastLeftJoin(leftOp, rightOp, leftKey, rightKey)
              else if (outer) then PhysicalOp.WideHashLeftJoin(leftOp, rightOp, leftKey, rightKey)
              else if (useBroadcast) then
                PhysicalOp.WideBroadcastJoin(leftOp, rightOp, leftKey, rightKey)
              else PhysicalOp.WideHashJoin(leftOp, rightOp, leftKey, rightKey)
            Some(chosen)
          case _ =>
            LoweringTrace.note("WideJoinOp", PhysicalOp.Kind.Columns, "WideLowering")
            None
      case _ =>
        None

  def group(
      source: com.ewoodbury.scarlet.core.Plan[WideRow],
      key: Int,
      measure: Int,
      key2: Option[Int],
      distinct: Option[Int],
      textMin: Option[Int],
      sum2: Option[Int],
      lowerChild: com.ewoodbury.scarlet.core.Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerChild(source).flatMap { child =>
      fieldsOf(child) match
        case Some(fields)
            if com.ewoodbury.scarlet.core.WideAggregate
              .output(fields, key, measure, key2, distinct, textMin, sum2)
              .isDefined =>
          Some(PhysicalOp.WideGroup(child, key, measure, key2, distinct, textMin, sum2))
        case Some(_) =>
          LoweringTrace.note("WideGroupOp", child.kind, "WideLowering")
          None
        case None =>
          None
    }

  def sort(
      source: com.ewoodbury.scarlet.core.Plan[WideRow],
      ordinal: Int,
      descending: Boolean,
      lowerChild: com.ewoodbury.scarlet.core.Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerChild(source).flatMap { child =>
      fieldsOf(child) match
        case Some(fields) if sortable(fields, ordinal) =>
          Some(PhysicalOp.WideSort(child, ordinal, descending))
        case Some(_) =>
          LoweringTrace.note("WideSortOp", child.kind, "WideLowering")
          None
        case None =>
          None
    }

  def limit(
      source: com.ewoodbury.scarlet.core.Plan[WideRow],
      count: Int,
      lowerChild: com.ewoodbury.scarlet.core.Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    if count < 0 then None
    else lowerChild(source).map(child => PhysicalOp.WideLimit(child, count))

  def project(
      source: com.ewoodbury.scarlet.core.Plan[WideRow],
      spec: Vector[(Int, String)],
      lowerChild: com.ewoodbury.scarlet.core.Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerChild(source).flatMap { child =>
      fieldsOf(child) match
        case Some(fields) if legal(spec, fields) =>
          Some(PhysicalOp.WideProject(child, spec))
        case _ =>
          LoweringTrace.note("WideProjectOp", child.kind, "WideLowering")
          None
    }

  private def sortable(fields: Vector[ColumnField], ordinal: Int): Boolean =
    fields.lift(ordinal).exists { field =>
      field.kind match
        case com.ewoodbury.scarlet.core.ColumnKind.Int32 |
            com.ewoodbury.scarlet.core.ColumnKind.Int64 |
            com.ewoodbury.scarlet.core.ColumnKind.Utf8 =>
          true
        case _ => false
    }

  private def column(op: PhysicalOp, ordinal: Int): Option[ColumnField] =
    fieldsOf(op).filter(fields => ordinal >= 0 && ordinal < fields.size).map(_(ordinal))

  private def legal(spec: Vector[(Int, String)], fields: Vector[ColumnField]): Boolean =
    spec.nonEmpty &&
      spec.forall { (ordinal, name) =>
        name.nonEmpty && ordinal >= 0 && ordinal < fields.size
      } &&
      spec.map(_._1).distinct.size == spec.size &&
      spec.map(_._2).distinct.size == spec.size

  private[execution] def fieldsOf(op: PhysicalOp): Option[Vector[ColumnField]] = op match
    case PhysicalOp.Scan(PhysicalOp.Kind.Columns, partitions) =>
      schema(partitions)
    case PhysicalOp.WideFilter(child, _, _) =>
      fieldsOf(child)
    case PhysicalOp.WideProject(child, spec) =>
      fieldsOf(child).map(fields => spec.map((ordinal, name) => fields(ordinal).copy(name = name)))
    case PhysicalOp.WideCompute(child, expr, name) =>
      fieldsOf(child).flatMap { fields =>
        com.ewoodbury.scarlet.core.ColumnExpr.resultKind(expr, fields).map { kind =>
          fields :+ com.ewoodbury.scarlet.core.ColumnField(name, kind)
        }
      }
    case PhysicalOp.WideHashJoin(left, right, _, rightKey) =>
      joinedFields(left, right, rightKey)
    case PhysicalOp.WideBroadcastJoin(left, right, _, rightKey) =>
      joinedFields(left, right, rightKey)
    case PhysicalOp.WideHashLeftJoin(left, right, _, rightKey) =>
      joinedFields(left, right, rightKey)
    case PhysicalOp.WideBroadcastLeftJoin(left, right, _, rightKey) =>
      joinedFields(left, right, rightKey)
    case PhysicalOp.WideGroup(child, key, measure, key2, distinct, textMin, sum2) =>
      fieldsOf(child).flatMap { fields =>
        com.ewoodbury.scarlet.core.WideAggregate
          .output(fields, key, measure, key2, distinct, textMin, sum2)
      }
    case PhysicalOp.WideSort(child, _, _) => fieldsOf(child)
    case PhysicalOp.WideLimit(child, _) => fieldsOf(child)
    case _ =>
      None

  private def joinedFields(
      left: PhysicalOp,
      right: PhysicalOp,
      rightKey: Int,
  ): Option[Vector[ColumnField]] =
    (fieldsOf(left), fieldsOf(right)) match
      case (Some(leftFields), Some(rightFields)) if rightKey >= 0 && rightKey < rightFields.size =>
        Some(leftFields ++ rightFields.zipWithIndex.collect {
          case (field, ordinal) if ordinal != rightKey => field
        })
      case _ =>
        None

  private def keysMatch(
      left: Vector[ColumnField],
      right: Vector[ColumnField],
      leftKey: Int,
      rightKey: Int,
  ): Boolean =
    columnKind(left, leftKey).exists { kind =>
      columnKind(right, rightKey).exists { other =>
        sameKind(kind, other) && (sameKind(kind, com.ewoodbury.scarlet.core.ColumnKind.Int32) ||
          sameKind(kind, com.ewoodbury.scarlet.core.ColumnKind.Int64))
      }
    }

  private def columnKind(fields: Vector[ColumnField], ordinal: Int): Option[
    com.ewoodbury.scarlet.core.ColumnKind,
  ] =
    if (ordinal >= 0 && ordinal < fields.size) then Some(fields(ordinal).kind) else None

  private def schema(
      partitions: Seq[com.ewoodbury.scarlet.core.Partition[_]],
  ): Option[Vector[ColumnField]] =
    val tables = partitions.map(_.data).collect { case columns: WideColumns => columns }
    if (tables.size != partitions.size || tables.isEmpty) then None
    else
      val fields = tables.head.fields
      if (tables.forall(table => same(table.fields, fields))) then Some(fields) else None

  private def same(left: Vector[ColumnField], right: Vector[ColumnField]): Boolean =
    left.size == right.size && left.zip(right).forall { (a, b) =>
      a.name.equals(b.name) && sameKind(a.kind, b.kind)
    }

  private def sameKind(
      left: com.ewoodbury.scarlet.core.ColumnKind,
      right: com.ewoodbury.scarlet.core.ColumnKind,
  ): Boolean =
    (left, right) match
      case (
            com.ewoodbury.scarlet.core.ColumnKind.Int32,
            com.ewoodbury.scarlet.core.ColumnKind.Int32,
          ) =>
        true
      case (
            com.ewoodbury.scarlet.core.ColumnKind.Int64,
            com.ewoodbury.scarlet.core.ColumnKind.Int64,
          ) =>
        true
      case (
            com.ewoodbury.scarlet.core.ColumnKind.Float64,
            com.ewoodbury.scarlet.core.ColumnKind.Float64,
          ) =>
        true
      case (
            com.ewoodbury.scarlet.core.ColumnKind.Bool,
            com.ewoodbury.scarlet.core.ColumnKind.Bool,
          ) =>
        true
      case (
            com.ewoodbury.scarlet.core.ColumnKind.Utf8,
            com.ewoodbury.scarlet.core.ColumnKind.Utf8,
          ) =>
        true
      case (
            com.ewoodbury.scarlet.core.ColumnKind.Float32,
            com.ewoodbury.scarlet.core.ColumnKind.Float32,
          ) =>
        true
      case _ =>
        false

  private type PlanNode = com.ewoodbury.scarlet.core.Plan[WideRow]
