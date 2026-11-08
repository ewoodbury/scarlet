package com.ewoodbury.scarlet.columnar

/**
 * A shape the columnar path declines on purpose, so the caller can use the row path.
 *
 * This extends [[ClassCastException]] so the existing fallback catches keep working, including a
 * cast that fails on a mistyped predicate. The class names the deliberate decline. A cast bug that
 * is not one of those declines stays a plain [[ClassCastException]]. A closed kernel that will not
 * run an input in memory is [[ClosedKernelRefusal]], not this decline, and the action fails.
 */
final class ColumnarFallback(message: String) extends ClassCastException(message)

/**
 * A closed-kernel operator will not run this input in memory.
 *
 * This does not extend [[ClassCastException]]. [[ColumnarFallback]] is a decline the row path can
 * run. This refusal is not one of those declines, so the action fails instead of rerunning on
 * rows. `rows` is the row count charged to the table, `bytes` is the in-memory estimate, and
 * `budget` is the byte budget that estimate exceeded. A spill file may already exist for an
 * earlier piece of the same input. The store is deleted, and the action still fails.
 */
final class ClosedKernelRefusal(val rows: Long, val bytes: Long, val budget: Long)
    extends RuntimeException(
      s"$rows input rows ($bytes bytes) exceed the $budget byte budget",
    )
