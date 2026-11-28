package com.ewoodbury.scarlet.core

/**
 * Output schema of a group over one wide-row column.
 *
 * A key is an int, a long, or a string. A numeric measure emits count, sum, min, and max. Count is
 * a long. Sum is a long for an int or long measure, and a double for a float or double measure.
 * Min and max keep the measure's kind. A string measure emits count, min, and max, and has no sum.
 * Those strings compare with JVM `compareTo`. `textMin` appends the min of one other string column
 * after max. A bool measure is not in this set. A null key is dropped. A null measure is counted
 * and is not a sum, min, or max. `distinct` appends one long column when that source column is an
 * int, a long, or a string.
 */
private[scarlet] object WideAggregate:

  def output(
      fields: Vector[ColumnField],
      key: Int,
      measure: Int,
      key2: Option[Int] = None,
      distinct: Option[Int] = None,
      textMin: Option[Int] = None,
      sum2: Option[Int] = None,
  ): Option[Vector[ColumnField]] =
    fieldAt(fields, key).flatMap { keyField =>
      secondField(fields, key, key2).flatMap { extra =>
        fieldAt(fields, measure).flatMap { measureField =>
          addedMin(fields, measure, measureField.kind, textMin).flatMap { second =>
            addedSum(fields, measure, measureField.kind, sum2).flatMap { extraSum =>
              if (second && extraSum) then None
              else if (groupKey(keyField.kind) && extra.forall(field => groupKey(field.kind))) then
                counted(fields, distinct).flatMap { extraCount =>
                  body(measureField, second, extraSum).map { middle =>
                    Vector(keyField) ++ extra.toVector ++ middle ++ extraCount
                  }
                }
              else None
            }
          }
        }
      }
    }

  /**
   * Refuses a second sum that is not one int or long column beside an int or long measure. A
   * missing sum is allowed. An average is that sum divided by the count.
   */
  def checkSum(
      fields: Vector[ColumnField],
      key: Int,
      measure: Int,
      key2: Option[Int],
      sum2: Option[Int],
  ): Unit = sum2.foreach { ordinal =>
    if (key2.isDefined) then throw new IllegalArgumentException("a second sum is one key")
    if (ordinal == measure) then
      throw new IllegalArgumentException("a second sum needs another column")
    if (!whole(fields, key)) then
      throw new IllegalArgumentException("a second sum needs an int or long key")
    if (!whole(fields, measure)) then
      throw new IllegalArgumentException("a second sum needs an int or long measure")
    if (!whole(fields, ordinal)) then
      throw new IllegalArgumentException("a second sum needs an int or long column")
  }

  /** True when the extra string min is emitted. None refuses the group. */
  private def addedMin(
      fields: Vector[ColumnField],
      measure: Int,
      measureKind: ColumnKind,
      textMin: Option[Int],
  ): Option[Boolean] = textMin match
    case None => Some(false)
    case Some(ordinal) if ordinal != measure =>
      fieldAt(fields, ordinal).flatMap { field =>
        if (measureKind == ColumnKind.Utf8 && field.kind == ColumnKind.Utf8) then Some(true)
        else None
      }
    case Some(_) => None

  /** True when the extra numeric sum is emitted. None refuses the group. */
  private def addedSum(
      fields: Vector[ColumnField],
      measure: Int,
      measureKind: ColumnKind,
      sum2: Option[Int],
  ): Option[Boolean] = sum2 match
    case None => Some(false)
    case Some(ordinal) if ordinal != measure && wholeKind(measureKind) =>
      fieldAt(fields, ordinal).flatMap { field =>
        if (wholeKind(field.kind)) then Some(true) else None
      }
    case Some(_) => None

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

  private def body(
      measure: ColumnField,
      second: Boolean,
      extraSum: Boolean,
  ): Option[Vector[ColumnField]] =
    summed(measure.kind) match
      case Some(sumKind) if !second && !extraSum =>
        Some(
          Vector(
            ColumnField("count", ColumnKind.Int64),
            ColumnField("sum", sumKind),
            ColumnField("min", measure.kind),
            ColumnField("max", measure.kind),
          ),
        )
      case Some(sumKind) if !second && extraSum =>
        Some(
          Vector(
            ColumnField("count", ColumnKind.Int64),
            ColumnField("sum", sumKind),
            ColumnField("min", measure.kind),
            ColumnField("max", measure.kind),
            ColumnField("sum2", ColumnKind.Int64),
          ),
        )
      case Some(_) => None
      case None =>
        measure.kind match
          case ColumnKind.Utf8 if !extraSum =>
            val columns = Vector(
              ColumnField("count", ColumnKind.Int64),
              ColumnField("min", ColumnKind.Utf8),
              ColumnField("max", ColumnKind.Utf8),
            )
            Some(if (second) then columns :+ ColumnField("min2", ColumnKind.Utf8) else columns)
          case _ => None

  private def whole(fields: Vector[ColumnField], ordinal: Int): Boolean =
    fieldAt(fields, ordinal).exists(field => wholeKind(field.kind))

  private def wholeKind(kind: ColumnKind): Boolean = kind match
    case ColumnKind.Int32 | ColumnKind.Int64 => true
    case _ => false

  private def summed(kind: ColumnKind): Option[ColumnKind] = kind match
    case ColumnKind.Int32 | ColumnKind.Int64 => Some(ColumnKind.Int64)
    case ColumnKind.Float64 | ColumnKind.Float32 => Some(ColumnKind.Float64)
    case ColumnKind.Bool | ColumnKind.Utf8 | ColumnKind.Decimal(_, _) => None
