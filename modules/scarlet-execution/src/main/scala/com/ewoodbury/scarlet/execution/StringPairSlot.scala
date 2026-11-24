package com.ewoodbury.scarlet.execution

/**
 * One key or value slot above one string-pair project.
 *
 * The project sits on a scan or a string-pair filter stack. Neither function is read. A second
 * project, a second slot, and a whole-row filter are not this shape. [[FilterChain.selected]]
 * declines a filter that covers the project, so a collect still publishes both columns. A stack of
 * two or more key or value slots above one string `mapValues` is not that decline. `countByKey` of
 * this one slot does not. `reduceByKey` of this one slot does not either: it inserts the projected
 * pair into the reduce table. A second map still publishes, and
 * [[com.ewoodbury.scarlet.columnar.StringHashCount.byKey]] hashes those batches. A bare project
 * under `reduceByKey` still publishes, and
 * [[com.ewoodbury.scarlet.columnar.StringHashReduce.byKey]] hashes that batch.
 */
private[execution] object StringPairSlot:

  /** One [[PhysicalOp.Project]] whose child is a string-pair scan or filter stack. */
  def oneProject(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Project(child)
        if (op.kind eq PhysicalOp.Kind.StringPairs) && scanOrFilters(child) =>
      true
    case _ => false

  /** A string-pair project anywhere under `op`, including `op` itself. */
  def containsProject(op: PhysicalOp): Boolean =
    isProject(op) || op.children.exists(containsProject)

  /** A filter whose child tree holds a string-pair project. */
  def filterCoversProject(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Filter(child, _) => containsProject(child)
    case _ => false

  private def isProject(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Project(_) if op.kind eq PhysicalOp.Kind.StringPairs => true
    case _ => false

  /** A scan of string pairs, or filters over that scan. A project stops the walk. */
  private def scanOrFilters(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Scan(scanned, _) if scanned eq PhysicalOp.Kind.StringPairs => true
    case PhysicalOp.Filter(child, _) => scanOrFilters(child)
    case _ => false
