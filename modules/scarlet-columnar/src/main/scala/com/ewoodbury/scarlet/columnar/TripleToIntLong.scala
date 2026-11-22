package com.ewoodbury.scarlet.columnar

/**
 * Projects one int-triple morsel to `[Int32, Int64]`.
 *
 * The function runs once per fully present triple in `[start, start + length)`. The declared shape
 * is `apply(Tuple2): Tuple2`; this does not look for `(Int, Long)`. A result that is not a pair of
 * `Int` and `Long` throws [[ColumnarFallback]] before the batch is published. A null component in
 * the range declines the same way: the row path would unbox it. An empty range publishes an empty
 * batch and does not call the function. Output rows are packed from index 0. Both columns are
 * present. Each column owns its validity words.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Throw",
    "org.wartremover.warts.Var",
  ),
)
object TripleToIntLong:

  /** Present cells are unboxed. The cast is not a call. */
  trait TripleMap:
    def apply(key: Int, left: Int, right: Int): Any

  /** One present `(Int, Long)`. The caller owns the group. A null cell is not passed. */
  private[scarlet] trait LongSink:
    def add(key: Int, value: Long): Unit

  /**
   * One key or value test on a projected `(Int, Long)`. The function is not read. A null key is
   * not passed to [[IntLongSlot.Key]]. A null value is not passed to [[IntLongSlot.Value]].
   */
  private[scarlet] enum IntLongSlot:
    case Key(keep: Int => Boolean)
    case Value(keep: Long => Boolean)

  def adapt(mapper: Any): TripleMap =
    val fn = mapper.asInstanceOf[((Int, (Int, Int))) => Any]
    new TripleMap:
      def apply(key: Int, left: Int, right: Int): Any = fn((key, (left, right)))

  def project(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      mapper: TripleMap,
  ): ColumnBatch =
    val (keys, leftValues, rightValues) = tripleColumns(batch, start, length)
    if (length == 0) then emptyBatch
    else if (present(keys, leftValues, rightValues, start, length)) then
      projectDense(keys, leftValues, rightValues, start, length, mapper)
    else decline("null component")

  private def projectDense(
      keys: Int32Column,
      leftValues: Int32Column,
      rightValues: Int32Column,
      start: Int,
      length: Int,
      mapper: TripleMap,
  ): ColumnBatch =
    val outKeys = new Array[Int](length)
    val outValues = new Array[Long](length)
    val keyArr = keys.values
    val leftArr = leftValues.values
    val rightArr = rightValues.values
    val end = start + length
    var row = start
    var out = 0
    while (row < end) {
      write(mapper(keyArr(row), leftArr(row), rightArr(row)), outKeys, outValues, out)
      out += 1
      row += 1
    }
    publish(outKeys, outValues, length)

  /**
   * One projected pair, not stored. A null, or a cell that is not `(Int, Long)`, declines with the
   * same name [[project]] uses. The caller has already rejected a null input component.
   */
  private[scarlet] def offer(projected: Any, sink: LongSink): Unit =
    projected match
      case null => decline("null")
      case row: Product if row.productArity == 2 =>
        sink.add(readKey(row.productElement(0)), readValue(row.productElement(1)))
      case other => decline(foundName(other))

  /**
   * [[offer]] after one key or value slot. A null pair still declines. A null projected key is
   * dropped and a key test is not called. A null projected value is dropped from a value test and
   * is not passed to it. A key test that keeps a present key still counts a null value: the count
   * sink ignores that cell. A value test that keeps a present value still drops a null key. A cell
   * that is not `(Int, Long)` declines with the same name as [[offer]].
   */
  private[scarlet] def offerSlot(projected: Any, slot: IntLongSlot, sink: LongSink): Unit =
    projected match
      case null => decline("null")
      case row: Product if row.productArity == 2 =>
        val valueCell = row.productElement(1)
        valueCell match
          case null | _: Long => ()
          case other => decline(foundName(other))
        val keyCell = row.productElement(0)
        keyCell match
          case null | _: Int => ()
          case other => decline(foundName(other))
        keepSlot(keyCell, valueCell, slot, sink)
      case other => decline(foundName(other))

  private def keepSlot(keyCell: Any, valueCell: Any, slot: IntLongSlot, sink: LongSink): Unit =
    slot match
      case IntLongSlot.Key(keep) =>
        keyCell match
          case key: Int if keep(key) => sink.add(key, valueOrZero(valueCell))
          case _ => ()
      case IntLongSlot.Value(keep) =>
        valueCell match
          case value: Long if keep(value) =>
            keyCell match
              case key: Int => sink.add(key, value)
              case _ => ()
          case _ => ()

  /** The count sink drops this cell. A null value still counts when the key slot keeps the key. */
  private def valueOrZero(cell: Any): Long = cell match
    case value: Long => value
    case _ => 0L

  /** A matched null component. [[project]] declines the same way before it calls the function. */
  private[scarlet] def rejectNull(): Nothing =
    decline("null component")

  private def write(projected: Any, keys: Array[Int], values: Array[Long], out: Int): Unit =
    projected match
      case null => decline("null")
      case row: Product if row.productArity == 2 =>
        writeKey(row.productElement(0), keys, out)
        writeValue(row.productElement(1), values, out)
      case other => decline(foundName(other))

  private def readKey(cell: Any): Int = cell match
    case present: Int => present
    case other => decline(foundName(other))

  private def readValue(cell: Any): Long = cell match
    case present: Long => present
    case other => decline(foundName(other))

  private def writeKey(cell: Any, into: Array[Int], out: Int): Unit = cell match
    case present: Int => into(out) = present
    case other => decline(foundName(other))

  private def writeValue(cell: Any, into: Array[Long], out: Int): Unit = cell match
    case present: Long => into(out) = present
    case other => decline(foundName(other))

  private def publish(outKeys: Array[Int], outValues: Array[Long], length: Int): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int64),
      Vector(
        Int32Column.of(outKeys, Validity.allValid(length), length),
        Int64Column.of(outValues, Validity.allValid(length), length),
      ),
      length,
    )

  private def emptyBatch: ColumnBatch = ColumnBatches.emptyIntLongSum

  private def present(
      keys: Int32Column,
      leftValues: Int32Column,
      rightValues: Int32Column,
      start: Int,
      length: Int,
  ): Boolean =
    Validity.rangePresent(keys.validity, start, length) &&
      Validity.rangePresent(leftValues.validity, start, length) &&
      Validity.rangePresent(rightValues.validity, start, length)

  private def tripleColumns(
      batch: ColumnBatch,
      start: Int,
      length: Int,
  ): (Int32Column, Int32Column, Int32Column) =
    require(
      batch.columns.length == 3,
      s"triple batch expected 3 columns, got ${batch.columns.length}",
    )
    val keys = int32At(batch, 0)
    val leftValues = int32At(batch, 1)
    val rightValues = int32At(batch, 2)
    require(leftValues.length == keys.length, "key and left value columns differ in length")
    require(rightValues.length == keys.length, "key and right value columns differ in length")
    Ranges.check(start, length, keys.length)
    (keys, leftValues, rightValues)

  private def int32At(batch: ColumnBatch, ordinal: Int): Int32Column =
    ColumnKernel.asInt32(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private def decline(found: String): Nothing =
    throw new ColumnarFallback(s"triple map did not stay (Int, Long): found $found")

  private def foundName(value: Any): String = value match
    case null => "null"
    case other => other.asInstanceOf[AnyRef].getClass.getName
