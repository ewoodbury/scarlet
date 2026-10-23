package com.ewoodbury.scarlet.columnar

import scala.annotation.tailrec

/**
 * One filter or pair rewrite applied while summing an int pair.
 *
 * Steps run in source order on each present pair. A keep that rejects the row drops it. A map
 * replaces the pair, and later steps see that pair. The sum then groups by the resulting key. The
 * caller drops a row with a null component before [[PairStep.valueAfter]] runs.
 */
sealed trait PairStep

object PairStep:

  final case class KeepKey(predicate: Int => Boolean) extends PairStep
  final case class KeepValue(predicate: Int => Boolean) extends PairStep
  final case class KeepRow(predicate: (Int, Int) => Boolean) extends PairStep
  final case class MapPair(mapper: (Int, Int) => (Int, Int)) extends PairStep

  /**
   * Value of a present pair after `steps`. `None` when a keep drops the row. A map replaces the
   * pair, and later steps see that pair.
   */
  private[scarlet] def valueAfter(key: Int, value: Int, steps: List[PairStep]): Option[Int] =
    @tailrec
    def loop(currentKey: Int, currentValue: Int, rest: List[PairStep]): Option[Int] =
      rest match
        case Nil => Some(currentValue)
        case KeepKey(predicate) :: tail =>
          if (predicate(currentKey)) loop(currentKey, currentValue, tail) else None
        case KeepValue(predicate) :: tail =>
          if (predicate(currentValue)) loop(currentKey, currentValue, tail) else None
        case KeepRow(predicate) :: tail =>
          if (predicate(currentKey, currentValue)) loop(currentKey, currentValue, tail) else None
        case MapPair(mapper) :: tail =>
          val projected = mapper(currentKey, currentValue)
          loop(projected._1, projected._2, tail)

    loop(key, value, steps)
