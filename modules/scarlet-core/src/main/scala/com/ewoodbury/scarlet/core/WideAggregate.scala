package com.ewoodbury.scarlet.core

/**
 * Output schema of a group over one wide-row column.
 *
 * The key is an int or a long. The measure is an int, a long, a double, or a float. Count is a
 * long. Sum is a long for an int or long measure, and a double for a float or double measure. Min
 * and max keep the measure's kind. A bool or string measure is not in this set. A null key is
 * dropped. A null measure is counted and is not a sum, min, or max.
 */
private[scarlet] object WideAggregate:

  def output(
      fields: Vector[ColumnField],
      key: Int,
      measure: Int,
  ): Option[Vector[ColumnField]] =
    fieldAt(fields, key).flatMap { keyField =>
      fieldAt(fields, measure).flatMap { measureField =>
        summed(measureField.kind).flatMap { sumKind =>
          if (groupKey(keyField.kind)) then
            Some(
              Vector(
                keyField,
                ColumnField("count", ColumnKind.Int64),
                ColumnField("sum", sumKind),
                ColumnField("min", measureField.kind),
                ColumnField("max", measureField.kind),
              ),
            )
          else None
        }
      }
    }

  private def fieldAt(fields: Vector[ColumnField], ordinal: Int): Option[ColumnField] =
    if (ordinal >= 0 && ordinal < fields.size) then Some(fields(ordinal)) else None

  private def groupKey(kind: ColumnKind): Boolean = kind match
    case ColumnKind.Int32 | ColumnKind.Int64 => true
    case ColumnKind.Float64 | ColumnKind.Bool | ColumnKind.Utf8 | ColumnKind.Float32 => false

  private def summed(kind: ColumnKind): Option[ColumnKind] = kind match
    case ColumnKind.Int32 | ColumnKind.Int64 => Some(ColumnKind.Int64)
    case ColumnKind.Float64 | ColumnKind.Float32 => Some(ColumnKind.Float64)
    case ColumnKind.Bool | ColumnKind.Utf8 => None
