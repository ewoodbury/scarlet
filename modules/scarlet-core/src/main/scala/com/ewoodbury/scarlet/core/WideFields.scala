package com.ewoodbury.scarlet.core

/**
 * Schema already stored on a wide plan, when every partition agrees.
 *
 * Used to reject an expression before a job starts. A plan whose schema is not on the source
 * returns [[None]], and execution checks again.
 */
private[scarlet] object WideFields:

  def known(plan: Plan[WideRow]): Option[Vector[ColumnField]] = plan match
    case Plan.Source(partitions, _) =>
      val tables = partitions.flatMap(_.data match
        case columns: WideColumns => Some(columns.fields)
        case _ => None)
      tables match
        case first +: rest
            if tables.size == partitions.size && rest.forall(fields => same(fields, first)) =>
          Some(first)
        case _ =>
          None
    case Plan.WideFilterOp(source, _, _) => known(source)
    case Plan.WideProjectOp(source, spec) =>
      known(source).flatMap { fields =>
        if (spec.forall((ordinal, _) => ordinal >= 0 && ordinal < fields.size)) then
          Some(spec.map((ordinal, name) => fields(ordinal).copy(name = name)))
        else None
      }
    case Plan.WideComputeOp(source, expr, name) =>
      known(source).flatMap { fields =>
        ColumnExpr.resultKind(expr, fields).map(kind => fields :+ ColumnField(name, kind))
      }
    case Plan.WideJoinOp(left, right, _, rightKey, _) =>
      (known(left), known(right)) match
        case (Some(leftFields), Some(rightFields))
            if rightKey >= 0 && rightKey < rightFields.size =>
          Some(leftFields ++ rightFields.zipWithIndex.collect {
            case (field, ordinal) if ordinal != rightKey => field
          })
        case _ =>
          None
    case Plan.WideGroupOp(source, key, measure) =>
      known(source).flatMap(fields => WideAggregate.output(fields, key, measure))
    case _ =>
      None

  private def same(left: Vector[ColumnField], right: Vector[ColumnField]): Boolean =
    left.size == right.size && left.zip(right).forall { (leftField, rightField) =>
      leftField.name.equals(rightField.name) && sameKind(leftField.kind, rightField.kind)
    }

  private def sameKind(left: ColumnKind, right: ColumnKind): Boolean = (left, right) match
    case (ColumnKind.Int32, ColumnKind.Int32) => true
    case (ColumnKind.Int64, ColumnKind.Int64) => true
    case (ColumnKind.Float64, ColumnKind.Float64) => true
    case (ColumnKind.Bool, ColumnKind.Bool) => true
    case (ColumnKind.Utf8, ColumnKind.Utf8) => true
    case (ColumnKind.Float32, ColumnKind.Float32) => true
    case _ => false
