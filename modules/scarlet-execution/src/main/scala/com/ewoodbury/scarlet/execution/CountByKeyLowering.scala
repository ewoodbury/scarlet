package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.execution.schema.ColumnBinding.Operator
import com.ewoodbury.scarlet.execution.schema.{ColumnBinding, KernelChoice, Layout}

/**
 * Lowers int, long, and string `countByKey`, and `countByKey` of a triple project.
 *
 * The child is an exchange, and the kernel performs the shuffle. A null key is dropped. The value
 * column is not read, so a null value still counts. An int child publishes
 * [[PhysicalOp.Kind.Pairs]]. A long child publishes [[PhysicalOp.Kind.LongCounts]], schema
 * `[Int64, Int32]`. A string child publishes [[PhysicalOp.Kind.StringPairs]]. A triple
 * [[PhysicalOp.Project]] reports `[Int32, Int64]` and is also [[PhysicalOp.CountByKey]]. The
 * exchange always wraps that project: the map may replace the join key. An int inner sort-merge or
 * broadcast under that project is rewritten to a hash join of two exchanges. Those nodes are not
 * emitted under the count. Collect of the join itself is unchanged. One filter of the triple row,
 * the triple key, or either triple value, between the project and that join, stays in place and
 * the join under it is rewritten the same way. One key or value filter immediately above the
 * project is kept, and a hinted join under that project is rewritten the same way. A second
 * filter, a second slot, and a sort-merge whose children are not both exchanges, stay on the row
 * path. The count is an `Int` and wraps. The long estimate charges 8-byte keys. The string
 * estimate charges dictionary bytes. The binder tag [[KernelChoice.CountByKey]] builds
 * [[PhysicalOp.CountByKey]] for an int, long, or string pair. An int inner hash, broadcast, or
 * sort-merge join is [[JoinAggregateBinding]]: [[PhysicalOp.CountByKey]] of a hash join of two
 * exchanges, with no extra exchange and no inserted project. A filter above the join, an outer
 * join, and a non-int join stay on the row path, as does every other kind, including
 * [[PhysicalOp.Kind.LongCounts]]. With spill off, an input that does not fit the budget throws
 * [[com.ewoodbury.scarlet.columnar.ClosedKernelRefusal]] and the action fails instead of rerunning
 * on rows. With spill on, an over-budget post-exchange bucket spills. A count table that still
 * does not fit throws [[com.ewoodbury.scarlet.columnar.ClosedKernelRefusal]]. The function body is
 * not read. [[JoinProbeAggregate]] is a different plan and is not built here.
 */
private[execution] object CountByKeyLowering:

  def apply(child: PhysicalOp): Option[PhysicalOp] =
    hashPartitionedProject(child) match
      case Some(projected) => Some(counted(projected))
      case None if intLongProject(child) => Some(counted(child))
      case None if intLongValue(child) => Some(counted(child))
      case None => JoinAggregateBinding.count(child).orElse(countedFromTag(child))

  /**
   * Pair count. [[KernelChoice.CountByKey]] builds [[PhysicalOp.CountByKey]].
   *
   * [[Layout.AssumedIntLong]] is not this arm. [[intLongProject]] already accepted or refused that
   * project, and the same tag would admit a hinted project this lowering refuses.
   */
  private def countedFromTag(child: PhysicalOp): Option[PhysicalOp] =
    if assumedIntLong(child) then
      LoweringTrace.note("CountByKeyOp", child.kind, "CountByKeyLowering")
      None
    else
      ColumnBinding.asked(child, Operator.CountByKey) match
        case Some(KernelChoice.CountByKey) => Some(counted(child))
        case _ =>
          LoweringTrace.note("CountByKeyOp", child.kind, "CountByKeyLowering")
          None

  /** The triple-project layout. Not an int, long, or string pair. */
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def assumedIntLong(child: PhysicalOp): Boolean =
    Layout.known(child) == Layout.AssumedIntLong

  private def counted(child: PhysicalOp): PhysicalOp =
    PhysicalOp.CountByKey(PhysicalOp.Exchange(child))

  /**
   * A triple project over an int inner sort-merge or broadcast join.
   *
   * Both sides are hash exchanges, then [[PhysicalOp.HashJoin]], and the project stays above that
   * join. One triple-slot filter between the project and the join is kept above the hash join. A
   * second filter is refused. One key or value filter above the project stays above the rewritten
   * join, and a filter between that project and the join is refused. A sort-merge whose children
   * are not both exchanges is refused. The function body is not read. [[JoinProbeAggregate]] is a
   * different plan and is not built here.
   */
  private def hashPartitionedProject(child: PhysicalOp): Option[PhysicalOp] = child match
    case PhysicalOp.Project(under) =>
      hashedUnder(under).map(joined => PhysicalOp.Project(joined))
    case PhysicalOp.Filter(PhysicalOp.Project(under), slot) if keyOrValue(slot) =>
      hashedInner(under).map { joined =>
        PhysicalOp.Filter(PhysicalOp.Project(joined), slot)
      }
    case _ => None

  /** At most one triple filter, then the hinted join. The filter predicate is not read. */
  private def hashedUnder(op: PhysicalOp): Option[PhysicalOp] = op match
    case PhysicalOp.Filter(inner, slot) if tripleSlot(slot) =>
      hashedInner(inner).map(joined => PhysicalOp.Filter(joined, slot))
    case other => hashedInner(other)

  private def tripleSlot(slot: PhysicalOp.Slot): Boolean = slot match
    case PhysicalOp.Slot.TripleKey | PhysicalOp.Slot.TripleLeftValue |
        PhysicalOp.Slot.TripleRightValue | PhysicalOp.Slot.TripleRow =>
      true
    case PhysicalOp.Slot.Element | PhysicalOp.Slot.Row | PhysicalOp.Slot.Key |
        PhysicalOp.Slot.Value =>
      false

  private def hashedInner(op: PhysicalOp): Option[PhysicalOp] = op match
    case PhysicalOp.SortMergeJoin(PhysicalOp.Exchange(left), PhysicalOp.Exchange(right)) =>
      Some(PhysicalOp.HashJoin(PhysicalOp.Exchange(left), PhysicalOp.Exchange(right)))
    case PhysicalOp.BroadcastJoin(left, right, _) =>
      Some(PhysicalOp.HashJoin(exchanged(left), exchanged(right)))
    case _ => None

  private def exchanged(op: PhysicalOp): PhysicalOp = op match
    case present: PhysicalOp.Exchange => present
    case other => PhysicalOp.Exchange(other)

  /**
   * A triple [[PhysicalOp.Project]] reports `[Int32, Int64]`. Kind stays triples. The function
   * body is not read. A join that is not a project does not match: its schema is three ints.
   *
   * A hinted join that [[hashPartitionedProject]] did not rewrite stays out. That is a second
   * filter, a filter that is not a triple slot, or a sort-merge whose children are not both
   * exchanges. One key or value slot above a project that is already a hash join is kept. A second
   * slot, a whole-row filter, and any other filter stay out.
   */
  private def intLongProject(child: PhysicalOp): Boolean = child match
    case PhysicalOp.Project(under) if hintedJoin(under) => false
    case PhysicalOp.Filter(PhysicalOp.Project(under), slot)
        if keyOrValue(slot) && !hintedJoin(under) =>
      true
    case PhysicalOp.Filter(_, _) => false
    case _ =>
      child.kind match
        case PhysicalOp.Kind.Triples => PhysicalSchema.isIntLong(child.schema)
        case _ => false

  /** Int-to-long `mapValues`. The count hashes the int key and does not read the long value. */
  private def intLongValue(child: PhysicalOp): Boolean = child match
    case _: PhysicalOp.IntLongMapValue => true
    case _ => false

  private def keyOrValue(slot: PhysicalOp.Slot): Boolean = slot match
    case PhysicalOp.Slot.Key | PhysicalOp.Slot.Value => true
    case _ => false

  private def hintedJoin(op: PhysicalOp): Boolean = op match
    case PhysicalOp.SortMergeJoin(_, _) | PhysicalOp.BroadcastJoin(_, _, _) => true
    case PhysicalOp.Filter(inner, _) => hintedJoin(inner)
    case _ => false
