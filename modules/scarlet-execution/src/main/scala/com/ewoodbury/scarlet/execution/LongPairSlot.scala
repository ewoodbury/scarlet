package com.ewoodbury.scarlet.execution

/**
 * One key or value slot above one long-pair [[PhysicalOp.Project]].
 *
 * The project sits on a long-pair scan or a long-pair filter stack. A second slot, a row filter, a
 * second project, and a project over a sort, join, or aggregate are not this shape. The predicate
 * is not read.
 */
private[execution] object LongPairSlot:

  /** One key or value slot whose child is that project. */
  def above(child: PhysicalOp, slot: PhysicalOp.Slot): Boolean = (child, slot) match
    case (PhysicalOp.Project(under), PhysicalOp.Slot.Key | PhysicalOp.Slot.Value) =>
      scanOrFilterStack(under)
    case _ => false

  /**
   * True when a long-pair project is still in `op`. A parent that would publish that project, or
   * stack another slot on the one [[above]] admits, declines.
   */
  def inTree(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Project(child) if longPairs(child) => true
    case other => other.children.exists(inTree)

  private def longPairs(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.LongPairs => true
    case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.StringPairs | PhysicalOp.Kind.Ints |
        PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats | PhysicalOp.Kind.Bools |
        PhysicalOp.Kind.Strings | PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongTriples |
        PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.LongCounts | PhysicalOp.Kind.GroupedInts |
        PhysicalOp.Kind.GroupedStrings | PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter |
        PhysicalOp.Kind.StringOuter | PhysicalOp.Kind.CountSums | PhysicalOp.Kind.IntLongSums =>
      false

  /** A long-pair scan, or key, value, and row filters over that scan. No project inside. */
  def scanOrFilterStack(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Scan(PhysicalOp.Kind.LongPairs, _) => true
    case PhysicalOp.Filter(child, _) => scanOrFilterStack(child)
    case _ => false
