package com.ewoodbury.scarlet.columnar

/**
 * Which value column of an outer-join batch may be absent.
 *
 * The batch stays `(key, left, right)`. [[AbsentSide.Left]] decodes `(key, (Option[Int], Int))`.
 * [[AbsentSide.Right]] decodes `(key, (Int, Option[Int]))`. [[AbsentSide.Both]] decodes
 * `(key, (Option[Int], Option[Int]))`. A clear bit is absence. A null value is not absence.
 */
enum AbsentSide:

  /** The left value may be absent. A right outer join publishes this side. */
  case Left

  /** The right value may be absent. A left outer join publishes this side. */
  case Right

  /** Both value columns may be absent. A full outer join publishes this side. */
  case Both
