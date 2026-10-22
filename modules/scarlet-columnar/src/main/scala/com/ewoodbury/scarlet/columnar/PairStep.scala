package com.ewoodbury.scarlet.columnar

/**
 * One filter or pair rewrite applied while summing an int pair.
 *
 * Steps run in source order on each present pair. A keep that rejects the row drops it. A map
 * replaces the pair, and later steps see that pair. The sum then groups by the resulting key.
 */
sealed trait PairStep

object PairStep:

  final case class KeepKey(predicate: Int => Boolean) extends PairStep
  final case class KeepValue(predicate: Int => Boolean) extends PairStep
  final case class KeepRow(predicate: (Int, Int) => Boolean) extends PairStep
  final case class MapPair(mapper: (Int, Int) => (Int, Int)) extends PairStep
