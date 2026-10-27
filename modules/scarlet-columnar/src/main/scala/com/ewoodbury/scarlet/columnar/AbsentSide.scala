package com.ewoodbury.scarlet.columnar

/**
 * Which value column of an outer-join batch may be absent.
 *
 * The batch stays `(key, left, right)`. [[AbsentSide.Left]] decodes `(key, (Option[Int], Int))`.
 * [[AbsentSide.Right]] decodes `(key, (Int, Option[Int]))`. Full outer, where both columns can be
 * absent in one row, is not a case.
 */
enum AbsentSide:

  /** The left value may be absent. A right outer join publishes this side. */
  case Left

  /** The right value may be absent. A left outer join publishes this side. */
  case Right
