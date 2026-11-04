package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{BatchCodec, ColumnBatch, ColumnKernel, Int32Column}
import com.ewoodbury.scarlet.core.{Partition, Plan}

import ColumnarInterpreter.Columns

/**
 * Encode, project, filter, and `mapValues` for `(String, Int)` batches.
 *
 * A batch is a UTF-8 dictionary key and an int value. A null key is a clear bit on the key and
 * stays in the batch. The value bit stays set, including on that row: a clear value bit is a null
 * `Int`, which this path does not lower. `mapValues` rewrites column 1 and copies the key bit
 * unchanged, so the new value does not inherit a clear key bit.
 *
 * [[encoded]] may publish a one-column batch when demand narrowed the read to the key or the
 * value. A [[PhysicalOp.Column]] or the filter path consumes that shape; a column wraps the batch
 * as [[Columns.Strings]] or [[Columns.Ints]]. [[BatchCodec.decodeStringPairs]] requires both
 * columns and is never called on a narrowed batch.
 *
 * Named erasure boundary. The physical slot chose the predicate, and lowering checked that
 * `mapValues` declares `Int => Int`.
 */
private[execution] object StringPairWalk:

  def encoded(partitions: Seq[Partition[_]], ordinals: Set[Int]): Columns =
    val batches = (ordinals.contains(0), ordinals.contains(1)) match
      case (true, false) =>
        partitions.map(partition => keysOnly(rowsOf(partition))).toVector
      case (false, true) =>
        partitions.map(partition => valuesOnly(rowsOf(partition))).toVector
      case _ =>
        partitions.map(partition => BatchCodec.stringIntPairs(rowsOf(partition))).toVector
    Columns.StringPairs(batches)

  def batches(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.StringPairs(parts) => parts
    case _ =>
      throw new IllegalStateException("columnar op expected string pairs")

  def wrapColumn(kind: PhysicalOp.Kind, columns: Vector[ColumnBatch], ordinal: Int): Columns =
    kind match
      case PhysicalOp.Kind.LongPairs => Columns.Longs(columns)
      case PhysicalOp.Kind.Pairs => Columns.Ints(columns)
      case PhysicalOp.Kind.StringPairs if ordinal == 0 => Columns.Strings(columns)
      case PhysicalOp.Kind.StringPairs => Columns.Ints(columns)
      case _ =>
        throw new IllegalStateException("columnar projection expected a pair")

  def mapValues(columns: Columns, mapper: Any): Columns =
    val fn = intMapper(mapper)
    Columns.StringPairs(
      ColumnarInterpreter.onEachMorsel(batches(columns)) { morsel =>
        ColumnKernel.projectInt32(
          morsel.batch,
          1,
          morsel.start,
          morsel.length,
          value => fn(value),
        )
      },
    )

  def filter(
      plan: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    val input = ColumnarInterpreter.interpretAt(
      sourceOf(plan),
      child,
      selectedPartition,
      preserveAbsence,
      ordinals,
    )
    val filtered = ColumnarInterpreter.onEachMorsel(batches(input)) { morsel =>
      applyFilter(plan, morsel.batch, morsel.start, morsel.length)
    }
    Columns.StringPairs(filtered)

  private def keysOnly(rows: Iterable[(String, Int)]): ColumnBatch =
    BatchCodec.strings(rows.map(pair => pair._1))

  private def valuesOnly(rows: Iterable[(String, Int)]): ColumnBatch =
    BatchCodec.ints(rows.map(pair => pair._2))

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def rowsOf(partition: Partition[_]): Iterable[(String, Int)] =
    // Classification already rejected a null value, so unboxing the int is safe.
    partition.data.asInstanceOf[Iterable[(String, Int)]]

  /**
   * The value column of a filter batch. A narrowed batch carries only the value, so it is the
   * first column; a full batch keeps the key at 0. A narrowed key batch is a planner bug, and the
   * type check turns it into a failure instead of filtering the string column as ints.
   */
  private def valueOrdinal(batch: ColumnBatch): Int =
    if (batch.columns.length == 1) then
      batch.columns.head match
        case _: Int32Column => 0
        case _ =>
          throw new IllegalStateException("string pair value filter expected an int column")
    else 1

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def intMapper(mapper: Any): Int => Int =
    // Lowering checked `Int => Int` through `returnsUnboxedInt`, which witnesses Kind.Ints.
    mapper.asInstanceOf[Int => Int]

  private def sourceOf(plan: Plan[_]): Plan[_] = plan match
    case Plan.FilterKeysOp(source, _) => source
    case Plan.FilterValuesOp(source, _) => source
    case _ =>
      throw new IllegalStateException("string pair filter expected a key or a value")

  private def applyFilter(
      plan: Plan[_],
      batch: ColumnBatch,
      start: Int,
      length: Int,
  ): ColumnBatch =
    plan match
      case Plan.FilterKeysOp(_, predicate) =>
        val keep = stringPredicate(predicate)
        ColumnKernel.filterUtf8(batch, 0, start, length, value => keep(value))
      case Plan.FilterValuesOp(_, predicate) =>
        val keep = intPredicate(predicate)
        val ordinal = valueOrdinal(batch)
        ColumnKernel.filterInt32(batch, ordinal, start, length, value => keep(value))
      case _ =>
        throw new IllegalStateException("string pair filter expected a key or a value")

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def stringPredicate(predicate: Any): String => Boolean =
    // The key slot chose this function. A null key is passed through.
    predicate.asInstanceOf[String => Boolean]

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def intPredicate(predicate: Any): Int => Boolean =
    // The value slot chose this function.
    predicate.asInstanceOf[Int => Boolean]
