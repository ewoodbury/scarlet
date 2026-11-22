package com.ewoodbury.scarlet.execution

/**
 * One key or value slot above one long-pair project.
 *
 * The project sits on a scan or a long-pair filter stack. Neither function is read. A second
 * project, a second slot, and a whole-row filter are not this shape. [[FilterChain.selected]]
 * stops at the project, so the slot indexes those projected batches and
 * [[com.ewoodbury.scarlet.columnar.RankSplitLong]] sorts that index.
 */
private[execution] object LongPairSlot:

  /** One [[PhysicalOp.Project]] whose child is a long-pair scan or filter stack. */
  def oneProject(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Project(child)
        if (op.kind eq PhysicalOp.Kind.LongPairs) && scanOrFilters(child) =>
      true
    case _ => false

  /** A long-pair project anywhere under `op`, including `op` itself. */
  def containsProject(op: PhysicalOp): Boolean =
    isProject(op) || op.children.exists(containsProject)

  private def isProject(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Project(_) if op.kind eq PhysicalOp.Kind.LongPairs => true
    case _ => false

  /** A scan of long pairs, or filters over that scan. A project stops the walk. */
  private def scanOrFilters(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Scan(scanned, _) if scanned eq PhysicalOp.Kind.LongPairs => true
    case PhysicalOp.Filter(child, _) => scanOrFilters(child)
    case _ => false
