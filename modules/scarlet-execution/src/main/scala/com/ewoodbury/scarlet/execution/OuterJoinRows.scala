package com.ewoodbury.scarlet.execution

/**
 * Shared grouping for shuffle-hash outer joins.
 *
 * Both sides are grouped after null keys are dropped, so a null key never matches. Each task
 * supplies the rows for a matched key, a left-only key, a right-only key, and each null-key row. A
 * side the join does not preserve returns an empty sequence. Matched keys are the caller's
 * cartesian product of the two value sequences. Each null-key row is passed once, unmatched.
 */
object OuterJoinRows:

  def emit[K, L, R, A](
      leftData: Seq[(K, L)],
      rightData: Seq[(K, R)],
      onMatch: (K, Seq[L], Seq[R]) => Seq[A],
      onLeftOnly: (K, Seq[L]) => Seq[A],
      onRightOnly: (K, Seq[R]) => Seq[A],
      onLeftNull: (K, L) => Seq[A],
      onRightNull: (K, R) => Seq[A],
  ): Seq[A] = {
    val leftByKey = group(leftData)
    val rightByKey = group(rightData)
    val grouped = (leftByKey.keySet union rightByKey.keySet).toSeq.flatMap { key =>
      (leftByKey.get(key), rightByKey.get(key)) match {
        case (Some(leftValues), Some(rightValues)) => onMatch(key, leftValues, rightValues)
        case (Some(leftValues), None) => onLeftOnly(key, leftValues)
        case (None, Some(rightValues)) => onRightOnly(key, rightValues)
        case (None, None) => Seq.empty[A]
      }
    }
    grouped ++
      nullRows(leftData).flatMap { case (key, value) => onLeftNull(key, value) } ++
      nullRows(rightData).flatMap { case (key, value) => onRightNull(key, value) }
  }

  private def group[K, V](rows: Seq[(K, V)]): Map[K, Seq[V]] =
    NullKeys
      .drop(rows)
      .groupBy(_._1)
      .view
      .mapValues(pairs => pairs.map(_._2).toSeq)
      .toMap

  /** Rows whose key is null, in input order. These are not part of [[group]]. */
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Equals"))
  private def nullRows[K, V](rows: Seq[(K, V)]): Seq[(K, V)] =
    rows.filter { case (key, _) => key == null }
