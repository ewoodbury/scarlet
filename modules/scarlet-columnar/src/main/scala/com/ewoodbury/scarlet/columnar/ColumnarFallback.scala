package com.ewoodbury.scarlet.columnar

/**
 * A shape the columnar path declines on purpose, so the caller can use the row path.
 *
 * This extends [[ClassCastException]] so the existing fallback catches keep working, including a
 * cast that fails on a mistyped predicate. The class names the deliberate decline. A cast bug that
 * is not one of those declines stays a plain [[ClassCastException]].
 */
final class ColumnarFallback(message: String) extends ClassCastException(message)
