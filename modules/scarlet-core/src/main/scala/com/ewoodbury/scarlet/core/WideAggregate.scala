package com.ewoodbury.scarlet.core

/**
 * Output schema of a group over one wide-row column.
 *
 * A key is an int, a long, or a string. A numeric measure emits count, sum, min, and max. Count is
 * a long. Sum is a long for an int or long measure, and a double for a float or double measure.
 * Min and max keep the measure's kind. A string measure emits count, min, and max, and has no sum.
 * Those strings compare with JVM `compareTo`. A bool measure is not in this set. A null key is
 * dropped. A null measure is counted and is not a sum, min, or max. `distinct` appends one long
 * column when that source column is an int, a long, or a string.
 */
private[scarlet] object WideAggregate:

  def output(
      fields: Vector[ColumnField],
      key: Int,
      measure: Int,
      key2: Option[Int] = None,
      distinct: Option[Int] = None,
  ): Option[Vector[ColumnField]] =
    fieldAt(fields, key).flatMap { keyField =>
      secondField(fields, key, key2).flatMap { extra =>
        fieldAt(fields, measure).flatMap { measureField =>
          if (groupKey(keyField.kind) && extra.forall(field => groupKey(field.kind))) then
            counted(fields, distinct).flatMap { extraCount =>
              body(measureField).map { middle =>
                Vector(keyField) ++ extra.toVector ++ middle ++ extraCount
              }
            }
          else None
        }
      }
    }

  /** The appended distinct column, or an empty vector when the group does not ask for one. */
  private def counted(
      fields: Vector[ColumnField],
      distinct: Option[Int],
  ): Option[Vector[ColumnField]] = distinct match
    case None => Some(Vector.empty)
    case Some(ordinal) =>
      fieldAt(fields, ordinal).flatMap { field =>
        if (distinctKind(field.kind)) then Some(Vector(ColumnField("distinct", ColumnKind.Int64)))
        else None
      }

  private def distinctKind(kind: ColumnKind): Boolean = kind match
    case ColumnKind.Int32 | ColumnKind.Int64 | ColumnKind.Utf8 => true
    case ColumnKind.Float64 | ColumnKind.Bool | ColumnKind.Float32 => false
    case ColumnKind.Decimal(_, _) => false

  private def secondField(
      fields: Vector[ColumnField],
      key: Int,
      key2: Option[Int],
  ): Option[Option[ColumnField]] = key2 match
    case None => Some(None)
    case Some(ordinal) if ordinal != key => fieldAt(fields, ordinal).map(Some(_))
    case Some(_) => None

  private def fieldAt(fields: Vector[ColumnField], ordinal: Int): Option[ColumnField] =
    if (ordinal >= 0 && ordinal < fields.size) then Some(fields(ordinal)) else None

  private def groupKey(kind: ColumnKind): Boolean = kind match
    case ColumnKind.Int32 | ColumnKind.Int64 | ColumnKind.Utf8 => true
    case ColumnKind.Float64 | ColumnKind.Bool | ColumnKind.Float32 => false
    case ColumnKind.Decimal(_, _) => false

  private def body(measure: ColumnField): Option[Vector[ColumnField]] =
    summed(measure.kind) match
      case Some(sumKind) =>
        Some(
          Vector(
            ColumnField("count", ColumnKind.Int64),
            ColumnField("sum", sumKind),
            ColumnField("min", measure.kind),
            ColumnField("max", measure.kind),
          ),
        )
      case None =>
        measure.kind match
          case ColumnKind.Utf8 =>
            Some(
              Vector(
                ColumnField("count", ColumnKind.Int64),
                ColumnField("min", ColumnKind.Utf8),
                ColumnField("max", ColumnKind.Utf8),
              ),
            )
          case _ => None

  private def summed(kind: ColumnKind): Option[ColumnKind] = kind match
    case ColumnKind.Int32 | ColumnKind.Int64 => Some(ColumnKind.Int64)
    case ColumnKind.Float64 | ColumnKind.Float32 => Some(ColumnKind.Float64)
    case ColumnKind.Bool | ColumnKind.Utf8 | ColumnKind.Decimal(_, _) => None
