package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.LogicalType

/**
 * Output schema of a [[PhysicalOp]]. [[PhysicalOp.Kind]] stays the admission enum.
 *
 * A project whose child kind is [[PhysicalOp.Kind.Triples]] reports `[Int32, Int64]`. The cells
 * are whatever the function returns; the schema is not read off the lambda. A filter reports its
 * child, so one slot above that project keeps the two-column schema. Every other operator reports
 * the schema of its kind. An exchange reports its child, so a project under an exchange keeps the
 * two-column schema. A hash aggregate of that schema reports the same two columns.
 */
private[execution] object PhysicalSchema:

  val IntLong: IndexedSeq[LogicalType] = Vector(LogicalType.Int32, LogicalType.Int64)

  def isIntLong(schema: IndexedSeq[LogicalType]): Boolean =
    val types = schema.iterator
    types.hasNext && isInt32(types.next()) && types.hasNext && isInt64(types.next()) &&
    !types.hasNext

  /** A triple [[PhysicalOp.Project]]. Its schema is [[IntLong]]. A filter of one is not. */
  def isIntLongProject(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Project(_) if isIntLong(of(op)) => true
    case _ => false

  def of(op: PhysicalOp): IndexedSeq[LogicalType] = op match
    case PhysicalOp.Project(child) if isTriples(child) =>
      IntLong
    case PhysicalOp.Filter(child, _) =>
      of(child)
    case PhysicalOp.Exchange(child) =>
      of(child)
    case PhysicalOp.HashAggregate(child, _) if isIntLong(of(child)) =>
      IntLong
    case other =>
      ofKind(other.kind)

  /**
   * True when the child schema is `[Int32, Int64]`.
   *
   * [[Describe]] does not read this. The stored [[PhysicalOp.HashAggregateTag]] is the ledger
   * label. [[ExecutionRoute]] does not call this test. [[ExecutionRoute.countArm]] still calls
   * [[isIntLong]] on the output schema.
   */
  def isIntLongAggregate(op: PhysicalOp): Boolean = op match
    case PhysicalOp.HashAggregate(child, _) if isIntLong(of(child)) => true
    case _ => false

  private def isTriples(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.Triples => true
    case _ => false

  private def isInt32(tpe: LogicalType): Boolean = tpe match
    case LogicalType.Int32 => true
    case _ => false

  private def isInt64(tpe: LogicalType): Boolean = tpe match
    case LogicalType.Int64 => true
    case _ => false

  def ofKind(kind: PhysicalOp.Kind): IndexedSeq[LogicalType] = kind match
    case PhysicalOp.Kind.Ints => Vector(LogicalType.Int32)
    case PhysicalOp.Kind.Longs => Vector(LogicalType.Int64)
    case PhysicalOp.Kind.Floats => Vector(LogicalType.Float64)
    case PhysicalOp.Kind.Bools => Vector(LogicalType.Bool)
    case PhysicalOp.Kind.Strings => Vector(LogicalType.Utf8Dict)
    case PhysicalOp.Kind.Pairs => Vector(LogicalType.Int32, LogicalType.Int32)
    case PhysicalOp.Kind.LongPairs => Vector(LogicalType.Int64, LogicalType.Int64)
    case PhysicalOp.Kind.StringPairs => Vector(LogicalType.Utf8Dict, LogicalType.Int32)
    case PhysicalOp.Kind.Triples =>
      Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32)
    case PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.LongOuter =>
      Vector(LogicalType.Int64, LogicalType.Int64, LogicalType.Int64)
    case PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.StringOuter =>
      Vector(LogicalType.Utf8Dict, LogicalType.Int32, LogicalType.Int32)
    case PhysicalOp.Kind.LongCounts => Vector(LogicalType.Int64, LogicalType.Int32)
    case PhysicalOp.Kind.CountSums =>
      Vector(LogicalType.Int32, LogicalType.Int64, LogicalType.Int64)
    case PhysicalOp.Kind.IntLongSums => IntLong
    case PhysicalOp.Kind.GroupedInts => Vector(LogicalType.Int32, LogicalType.Int32)
    case PhysicalOp.Kind.GroupedLongs => Vector(LogicalType.Int64, LogicalType.Int64)
    case PhysicalOp.Kind.GroupedStrings => Vector(LogicalType.Utf8Dict, LogicalType.Int32)
