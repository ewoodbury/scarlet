package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Boxed int-pair encode that keeps a null component absent.
 *
 * One pass over the tuples. The first null allocates a doubling hole list, and [[Validity.except]]
 * clears those bits. A column with no nulls stays [[Validity.allValid]] and allocates no hole
 * list. A null slot stores `0`. Any other non-int cell throws [[IllegalArgumentException]]. This
 * path does not call [[ColumnMetrics.primitiveEncode]]. Building the [[ColumnBatch]] still notes
 * one batch.
 */
private[scarlet] object IntPairEncode:

  /** Both columns. `role` in a rejection is `key` or `value`. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def pairs(values: Iterable[(Int, Int)]): ColumnBatch =
    val count = values.size
    val leftValues = new Array[Int](count)
    val rightValues = new Array[Int](count)
    val rows = values.iterator
    val left = new PairHoles
    val right = new PairHoles
    var row = 0
    while (row < count) {
      val pair = rows.next()
      write(pair, 0, "key", leftValues, left, row)
      write(pair, 1, "value", rightValues, right, row)
      row += 1
    }
    new ColumnBatch(
      schema = Vector(LogicalType.Int32, LogicalType.Int32),
      columns = Vector(
        Int32Column.of(leftValues, left.validity(count), count),
        Int32Column.of(rightValues, right.validity(count), count),
      ),
      length = count,
    )

  /**
   * One component, published as a single `Int32` column. `ordinal` 0 is the key and any other
   * ordinal is the value. The other component is not read.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def side(values: Iterable[Product], ordinal: Int): ColumnBatch =
    val role = if (ordinal == 0) "key" else "value"
    val count = values.size
    val stored = new Array[Int](count)
    val rows = values.iterator
    val holes = new PairHoles
    var row = 0
    while (row < count) {
      write(rows.next(), ordinal, role, stored, holes, row)
      row += 1
    }
    new ColumnBatch(
      schema = Vector(LogicalType.Int32),
      columns = Vector(Int32Column.of(stored, holes.validity(count), count)),
      length = count,
    )

  /**
   * Writes a present `Int`. A null cell is a hole. Any other non-int fails. Named erasure
   * boundary: `productElement` does not unbox an `(Int, Int)` cell, so a null stays absent.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def write(
      pair: Product,
      ordinal: Int,
      role: String,
      into: Array[Int],
      holes: PairHoles,
      row: Int,
  ): Unit =
    pair.productElement(ordinal) match
      case null => holes.note(row)
      case value: Int => into(row) = value
      case other =>
        val found = other.asInstanceOf[AnyRef].getClass.getName
        throw new IllegalArgumentException(s"pair component expected an Int $role, found $found")

  /** Hole rows, allocated on the first null and doubled after that. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class PairHoles:
    private var holes = Option.empty[Array[Int]]
    private var count = 0

    def note(row: Int): Unit =
      val current = holes match
        case Some(existing) if count < existing.length => existing
        case Some(existing) =>
          val grown = Arrays.copyOf(existing, existing.length * 2)
          holes = Some(grown)
          grown
        case None =>
          val fresh = new Array[Int](8)
          holes = Some(fresh)
          fresh
      current(count) = row
      count += 1

    def validity(width: Int): Validity = holes match
      case None => Validity.allValid(width)
      case Some(recorded) => Validity.except(width, recorded, count)
