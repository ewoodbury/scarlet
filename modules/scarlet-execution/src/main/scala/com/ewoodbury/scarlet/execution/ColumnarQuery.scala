package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import com.ewoodbury.scarlet.core.Plan

/**
 * Run a physical tree the planner already accepts.
 *
 * Count, sum, and take do not decode a `Seq`. The logical plan still carries the functions.
 * [[com.ewoodbury.scarlet.api.DistCollection]] reaches this object for those three actions, and
 * collect stays a decode. A tree this object did not already know how to run is not given a new
 * lowering. A decline returns [[scala.None]], and the caller reruns that action on rows.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Throw"))
private[execution] object ColumnarQuery:

  private val runs = new AtomicInteger

  /** Count, sum, and take calls since [[resetForTest]]. */
  def runsForTest: Int = runs.get()

  def resetForTest(): Unit =
    runs.set(0)

  /** Count finished rows. One int filter publishes the batches it counts. */
  def count(plan: Plan[_], op: PhysicalOp): Option[Long] =
    enter()
    try
      TrailingProject
        .count(plan, op)
        .orElse(
          Option.when(ColumnarPlanner.countsFilteredPairs(plan, op))(
            ColumnarPlanner.countFilteredPairs(plan, op),
          ),
        )
        .orElse(ColumnarPlanner.finishedCount(plan, op))
    // ColumnarFallback extends ClassCastException, so the catch above still sees a decline.
    catch case _: ClassCastException => None

  /** Sum an int column. `None` when the tree is not ints. */
  def sumInt(plan: Plan[_], op: PhysicalOp): Option[Long] =
    when(ColumnarPlanner.isInt(op))(ColumnarPlanner.sumInt(plan, op))

  /** Sum a long column. The total wraps. `None` when the tree is not longs. */
  def sumLong(plan: Plan[_], op: PhysicalOp): Option[Long] =
    when(ColumnarPlanner.isLong(op))(ColumnarPlanner.sumLong(plan, op))

  /** Sum a double column. `None` when the tree is not doubles. */
  def sumDouble(plan: Plan[_], op: PhysicalOp): Option[Double] =
    when(ColumnarPlanner.isFloat(op))(ColumnarPlanner.sumDouble(plan, op))

  /** Sum a float32 column. The total is a `Float`. `None` when the tree is not float32. */
  def sumFloat(plan: Plan[_], op: PhysicalOp): Option[Float] =
    when(op.kind eq PhysicalOp.Kind.Float32s)(Float32Sum.of(plan, op))

  /**
   * First `n` rows. `n <= 0` is empty and does not run. The rows are the prefix the interpreter
   * already returns, not a second decoder.
   */
  def take(plan: Plan[_], op: PhysicalOp, n: Int): Option[Seq[Any]] =
    enter()
    if (n <= 0) then Some(Seq.empty)
    else
      try ColumnarPlanner.takeAccepted(plan, op, n)
      // ColumnarFallback extends ClassCastException, so the catch above still sees a decline.
      catch case _: ClassCastException => None

  private def when[A](matches: Boolean)(value: => A): Option[A] =
    enter()
    if (!matches) then None
    else
      try Some(value)
      catch case _: ClassCastException => None

  private def enter(): Unit =
    val _ = runs.incrementAndGet()
