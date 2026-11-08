package com.ewoodbury.scarlet.columnar

/**
 * Which value column of an outer-join batch may be absent.
 *
 * The batch stays `(key, left, right)`. [[AbsentSide.Left]] decodes `(key, (Option[Int], Int))`.
 * [[AbsentSide.Right]] decodes `(key, (Int, Option[Int]))`. [[AbsentSide.Both]] decodes
 * `(key, (Option[Int], Option[Int]))`. An absence bit is a missing partner. A clear validity bit
 * is a null value, which this path does not publish.
 */
enum AbsentSide:

  /** The left value may be absent. A right outer join publishes this side. */
  case Left

  /** The right value may be absent. A left outer join publishes this side. */
  case Right

  /** Both value columns may be absent. A full outer join publishes this side. */
  case Both
