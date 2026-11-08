package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan}

import ColumnarInterpreter.Columns

/**
 * Encode, project, filter, and `mapValues` for `(String, Int)` batches.
 *
 * A batch is a UTF-8 dictionary key and an int value. A null key is a clear bit on the key and
 * stays in the batch. A null int is a clear value bit and is not stored as zero. `mapValues`
 * rewrites column 1 and copies both bits, so a null int stays null and the new value does not
 * inherit a clear key bit. A value projection of a null int declines, including one after a filter
 * or `mapValues` that kept the null.
 *
 * [[encoded]] may publish a one-column batch when demand narrowed the read to the key or the
 * value. A [[PhysicalOp.Column]] or the filter path consumes that shape; a column wraps the batch
 * as [[Columns.Strings]] or [[Columns.Ints]]. [[BatchCodec.decodeStringPairs]] reads both columns
 * and is never called on a narrowed batch. A whole-row stack is one [[Selection]] pass over both
 * columns. A key or value stack stays on the dense kernels below.
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

  /** A live null int cannot be joined, reduced, or grouped. The row path runs instead. */
  def refuseNullValues(batches: Vector[ColumnBatch]): Unit =
    if (!ColumnPipeline.valuesFullyPresent(batches)) then
      throw new ColumnarFallback("string pair value is null")

  /** Same decline as [[refuseNullValues]], on `rows(0 until count)` only. */
  def refuseSelectedNullValues(selected: Vector[Selected]): Unit =
    if (!ColumnPipeline.valuesFullyPresent(selected)) then
      throw new ColumnarFallback("string pair value is null")

  def batches(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.StringPairs(parts) => parts
    case _ =>
      throw new IllegalStateException("columnar op expected string pairs")

  def wrapColumn(kind: PhysicalOp.Kind, columns: Vector[ColumnBatch], ordinal: Int): Columns =
    kind match
      case PhysicalOp.Kind.LongPairs => Columns.Longs(columns)
      case PhysicalOp.Kind.Pairs => Columns.Ints(columns)
      case PhysicalOp.Kind.StringPairs if ordinal == 0 => Columns.Strings(columns)
      case PhysicalOp.Kind.StringPairs =>
        // A kept null int is not an Int32 column. Decline before decode throws.
        if (!ColumnPipeline.intsFullyPresent(columns)) then
          throw new ColumnarFallback("string pair value is null")
        Columns.Ints(columns)
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
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    op match
      case PhysicalOp.Filter(child, _) =>
        PredicatePlan.compile(plan, op) match
          case Some(compiled) if compiled.atoms.exists(isStringRow) =>
            selectRows(plan, op, compiled, selectedPartition, preserveAbsence, ordinals)
          case _ =>
            filterSlot(plan, child, selectedPartition, preserveAbsence, ordinals)
      case _ =>
        throw new IllegalStateException("string pair filter expected a key or a value")

  private def filterSlot(
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

  /** One pass over the source under a stack that contains a whole-row atom. */
  private def selectRows(
      plan: Plan[_],
      op: PhysicalOp,
      compiled: PredicatePlan.Compiled,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    val steps = PredicatePlan
      .selectionSteps(PredicatePlan.encoded(compiled, ordinals))
      .getOrElse(missingRows)
    val (source, child) = PredicatePlan.childOf(plan, op)
    val input = ColumnarInterpreter.interpretAt(
      source,
      child,
      selectedPartition,
      preserveAbsence,
      ordinals,
    )
    val filtered = ColumnarInterpreter.onEachMorsel(batches(input)) { morsel =>
      Selection(morsel.batch, morsel.start, morsel.length, steps)
    }
    Columns.StringPairs(filtered)

  private def isStringRow(atom: PredicatePlan.Atom): Boolean = atom match
    case PredicatePlan.Atom.StringRow(_) => true
    case PredicatePlan.Atom.Int32(_, _) | PredicatePlan.Atom.Int64(_, _) |
        PredicatePlan.Atom.Float64(_, _) | PredicatePlan.Atom.Bool(_, _) |
        PredicatePlan.Atom.Utf8(_, _) | PredicatePlan.Atom.Utf8Literals(_, _) |
        PredicatePlan.Atom.PairRow(_) | PredicatePlan.Atom.TripleRow(_) |
        PredicatePlan.Atom.LongPairRow(_) | PredicatePlan.Atom.LongPairKey(_) |
        PredicatePlan.Atom.LongPairValue(_, _) | PredicatePlan.Atom.PairKey(_) |
        PredicatePlan.Atom.PairValue(_, _) =>
      false

  private def missingRows: Nothing =
    throw new IllegalStateException("string pair row filter lost its selection")

  private def keysOnly(rows: Iterable[(String, Int)]): ColumnBatch =
    BatchCodec.strings(rows.map(pair => pair._1))

  private def valuesOnly(rows: Iterable[(String, Int)]): ColumnBatch =
    BatchCodec.ints(rows.map(presentInt))

  /**
   * A present int. A null int declines: [[BatchCodec.ints]] would unbox it to zero.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def presentInt(pair: (String, Int)): Int =
    pair.productElement(1) match
      case value: Int => value
      case _ => throw new ColumnarFallback("string pair value is null")

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def rowsOf(partition: Partition[_]): Iterable[(String, Int)] =
    // The encoder reads the value with productElement, so a null int is not unboxed here.
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
