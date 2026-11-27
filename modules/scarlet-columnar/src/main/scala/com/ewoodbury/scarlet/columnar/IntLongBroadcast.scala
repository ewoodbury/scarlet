package com.ewoodbury.scarlet.columnar

/**
 * Broadcast joins of an `Int32` key and an `Int64` value on both sides.
 *
 * The right side fills one table and each left batch probes it. There is no exchange and no spill.
 * An empty probe returns no batches. Output is `(key, left, right)`. A null key does not match. A
 * null long is a clear validity bit, not absence. [[IntLongHashJoin.inner]] writes the build value
 * in column 1, so an inner join swaps the value columns back. Outer joins call
 * [[IntLongHashOuter]] and already publish `(key, left, right)`. A full outer runs once, so
 * unmatched build rows are not repeated for every probe batch.
 */
object IntLongBroadcast:

  def inner(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (left.isEmpty || right.isEmpty) then Vector.empty
    else
      val built = ColumnBatches.concat(right)
      MorselScheduler.map(left, parallelism) { (_, batch) =>
        restore(
          IntLongHashJoin.inner(
            HashJoin.JoinSide(built, key = 0, value = 1),
            HashJoin.JoinSide(batch, key = 0, value = 1),
          ),
        )
      }

  def leftOuter(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (left.isEmpty) then Vector.empty
    else
      val built = side(right)
      MorselScheduler.map(left, parallelism) { (_, batch) =>
        IntLongHashOuter.leftOuter(batch, built)
      }

  def rightOuter(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (right.isEmpty) then Vector.empty
    else
      val built = side(left)
      MorselScheduler.map(right, parallelism) { (_, batch) =>
        IntLongHashOuter.rightOuter(built, batch)
      }

  def fullOuter(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (left.isEmpty && right.isEmpty) then Vector.empty
    else Vector(IntLongHashOuter.fullOuter(side(left), side(right)))

  /** The right side is the build. Swapping puts the left value back in column 1. */
  private def restore(batch: ColumnBatch): ColumnBatch =
    val columns = batch.columns
    new ColumnBatch(batch.schema, Vector(columns(0), columns(2), columns(1)), batch.length)

  /** [[ColumnBatches.concat]] throws on an empty vector. An empty side is one empty batch. */
  private def side(batches: Vector[ColumnBatch]): ColumnBatch =
    if (batches.isEmpty) then IntLongHashOuter.emptyBatch
    else ColumnBatches.concat(batches)
