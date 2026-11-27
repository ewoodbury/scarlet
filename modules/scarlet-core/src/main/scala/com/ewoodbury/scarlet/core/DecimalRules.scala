package com.ewoodbury.scarlet.core

/**
 * Exact arithmetic on a decimal stored as a scaled long.
 *
 * Precision is 1 through 18. Scale is 0 through that precision. The long is the unscaled value,
 * multiplied by `10^scale`. There is no binary fraction and no double. A result whose type or
 * value needs a 19th digit refuses. Divide is not in this set.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object DecimalRules:

  val MaxPrecision: Int = 18

  def check(precision: Int, scale: Int): Unit =
    require(
      precision >= 1 && precision <= MaxPrecision,
      s"decimal precision $precision",
    )
    require(
      scale >= 0 && scale <= precision,
      s"decimal scale $scale outside precision $precision",
    )

  /** `10^scale` for a scale this set accepts. */
  def pow10(scale: Int): Long =
    require(scale >= 0 && scale <= MaxPrecision, s"decimal power $scale")
    powers(scale)

  /** True when `unscaled` has at most `precision` digits. Zero fits every precision. */
  def fits(unscaled: Long, precision: Int): Boolean =
    require(precision >= 1 && precision <= MaxPrecision, s"decimal precision $precision")
    val limit = pow10(precision)
    if (unscaled >= 0L) then unscaled < limit else unscaled > -limit

  /**
   * Result kind of an add or a subtract. Scale is the larger scale. Precision is the larger
   * integer width, plus that scale, plus one digit for the carry. [[None]] when that precision is
   * past 18.
   */
  def addKind(
      leftPrecision: Int,
      leftScale: Int,
      rightPrecision: Int,
      rightScale: Int,
  ): Option[ColumnKind] =
    checked(leftPrecision, leftScale, rightPrecision, rightScale) {
      val scale = math.max(leftScale, rightScale)
      val precision = math.max(leftPrecision - leftScale, rightPrecision - rightScale) + scale + 1
      kindOf(precision, scale)
    }

  /** Result kind of a multiply. Precision and scale are the sums. [[None]] past 18 digits. */
  def mulKind(
      leftPrecision: Int,
      leftScale: Int,
      rightPrecision: Int,
      rightScale: Int,
  ): Option[ColumnKind] =
    checked(leftPrecision, leftScale, rightPrecision, rightScale) {
      kindOf(leftPrecision + rightPrecision, leftScale + rightScale)
    }

  def add(
      left: Long,
      leftPrecision: Int,
      leftScale: Int,
      right: Long,
      rightPrecision: Int,
      rightScale: Int,
  ): (Long, ColumnKind) =
    apply(addKind(leftPrecision, leftScale, rightPrecision, rightScale)) { kind =>
      val scale = scaleOf(kind)
      val sum = addExact(align(left, scale - leftScale), align(right, scale - rightScale))
      store(sum, kind)
    }

  def subtract(
      left: Long,
      leftPrecision: Int,
      leftScale: Int,
      right: Long,
      rightPrecision: Int,
      rightScale: Int,
  ): (Long, ColumnKind) =
    add(left, leftPrecision, leftScale, negate(right), rightPrecision, rightScale)

  def multiply(
      left: Long,
      leftPrecision: Int,
      leftScale: Int,
      right: Long,
      rightPrecision: Int,
      rightScale: Int,
  ): (Long, ColumnKind) =
    apply(mulKind(leftPrecision, leftScale, rightPrecision, rightScale)) { kind =>
      store(multiplyExact(left, right), kind)
    }

  /** Negative when `left` is below `right` after the scales are aligned. */
  def compare(left: Long, leftScale: Int, right: Long, rightScale: Int): Int =
    val scale = math.max(leftScale, rightScale)
    java.lang.Long.compare(align(left, scale - leftScale), align(right, scale - rightScale))

  /** `value` as an unscaled decimal. The integer gains `scale` zeros. */
  def fromLong(value: Long, precision: Int, scale: Int): Long =
    check(precision, scale)
    val unscaled = align(value, scale)
    if (!fits(unscaled, precision)) then refuse(precision)
    unscaled

  /** Drops the fractional digits toward zero. */
  def toLong(unscaled: Long, scale: Int): Long =
    require(scale >= 0 && scale <= MaxPrecision, s"decimal power $scale")
    if (scale == 0) then unscaled else unscaled / pow10(scale)

  private def checked(
      leftPrecision: Int,
      leftScale: Int,
      rightPrecision: Int,
      rightScale: Int,
  )(kind: => Option[ColumnKind]): Option[ColumnKind] =
    check(leftPrecision, leftScale)
    check(rightPrecision, rightScale)
    kind

  private def kindOf(precision: Int, scale: Int): Option[ColumnKind] =
    if (precision < 1 || precision > MaxPrecision || scale < 0 || scale > precision) then None
    else Some(ColumnKind.Decimal(precision, scale))

  private def apply(
      kind: Option[ColumnKind],
  )(body: ColumnKind => (Long, ColumnKind)): (Long, ColumnKind) =
    body(kind.getOrElse(throw new IllegalArgumentException("decimal precision exceeds 18")))

  private def store(unscaled: Long, kind: ColumnKind): (Long, ColumnKind) = kind match
    case ColumnKind.Decimal(precision, _) =>
      if (!fits(unscaled, precision)) then refuse(precision)
      (unscaled, kind)
    case other =>
      throw new IllegalArgumentException(s"decimal result $other")

  private def scaleOf(kind: ColumnKind): Int = kind match
    case ColumnKind.Decimal(_, scale) => scale
    case other => throw new IllegalArgumentException(s"decimal result $other")

  private def align(unscaled: Long, gap: Int): Long =
    if (gap == 0) then unscaled
    else if (gap < 0) then throw new IllegalArgumentException(s"decimal scale gap $gap")
    else multiplyExact(unscaled, pow10(gap))

  private def negate(value: Long): Long =
    try Math.negateExact(value)
    catch case _: ArithmeticException => refuse(MaxPrecision)

  private def addExact(left: Long, right: Long): Long =
    try Math.addExact(left, right)
    catch case _: ArithmeticException => refuse(MaxPrecision)

  private def multiplyExact(left: Long, right: Long): Long =
    try Math.multiplyExact(left, right)
    catch case _: ArithmeticException => refuse(MaxPrecision)

  private def refuse(precision: Int): Nothing =
    throw new IllegalArgumentException(s"decimal value does not fit precision $precision")

  private val powers: Array[Long] = Array.tabulate(MaxPrecision + 1) { scale =>
    var value = 1L
    var index = 0
    while (index < scale) {
      value = Math.multiplyExact(value, 10L)
      index += 1
    }
    value
  }
