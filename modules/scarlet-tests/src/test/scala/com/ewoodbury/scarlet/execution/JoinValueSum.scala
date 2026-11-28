package com.ewoodbury.scarlet.execution
import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnBatch, Int32Column}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.support.Check

/**
 * Sum of a join's value pair: `left * 31 + right` over every match.
 *
 * The plan is the same int shuffle-hash join `filter-join` counts. [[MatchCount]] sums chain
 * lengths and does not write the match columns. An int [[PhysicalOp.HashJoin]] of two exchanges
 * folds the same pair from the probe: each present hit adds `build * 31 + probe`, and a null value
 * is skipped, which is what `SUM` does. That path does not write the match columns. [[ofColumns]]
 * still sums triples a caller already wrote. Any other plan is interpreted, and a plan that does
 * not run as int triples uses the collected rows and the same arithmetic. Addition is commutative,
 * including overflow, so partition order does not change the result.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Equals"))
private[execution] object JoinValueSum:

  def of(joined: DistCollection[(Int, (Int, Int))]): Long =
    arrays(joined).getOrElse(collected(joined.collect()))

  /** Sum of triples a caller already wrote. The in-probe fold does not use this. */
  private[execution] def ofColumns(parts: Vector[ColumnBatch]): Long =
    sumParts(parts)

  /** Columnar sum. `None` keeps the collected recipe. */
  private def arrays(joined: DistCollection[(Int, (Int, Int))]): Option[Long] =
    if (!ScarletConf.get.columnarExecution) then None
    else if (ColumnarPlanner.partitioning(joined.plan).isEmpty) then None
    else
      ColumnarPlanner.lower(joined.plan) match
        case None => None
        case Some(op) =>
          folded(joined.plan, op).orElse(
            summed(ColumnarInterpreter.interpret(joined.plan, op)),
          )

  /** In-probe fold for the shuffle-hash inner join. Other shapes keep the written columns. */
  private def folded(plan: Plan[_], op: PhysicalOp): Option[Long] =
    (plan, op) match
      case (
            Plan.JoinOp(left, right, strategy),
            PhysicalOp.HashJoin(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)),
          ) if JoinLowering.acceptsShuffleHash(strategy) =>
        Some(
          JoinArms.sumValuePair(
            left,
            right,
            leftOp,
            rightOp,
            None,
            op.requestsTripleAbsence,
            ColumnUse.ordinals(plan, op),
          ),
        )
      case _ =>
        None

  private def summed(columns: ColumnarInterpreter.Columns): Option[Long] =
    columns match
      case ColumnarInterpreter.Columns.Triples(parts) => Some(sumParts(parts))
      case _ => None

  private def sumParts(parts: Vector[ColumnBatch]): Long =
    var index = 0
    var sum = 0L
    while (index < parts.length) {
      sum += add(Check.at(parts, index))
      index += 1
    }
    sum

  private def add(batch: ColumnBatch): Long =
    (intColumn(batch, 1), intColumn(batch, 2)) match
      case (Some(left), Some(right))
          if present(left, batch.length) && present(right, batch.length) =>
        sumArrays(left.values, right.values, batch.length)
      case (Some(left), Some(right)) =>
        sumSparse(left, right, batch.length)
      case _ =>
        throw new IllegalStateException("join value sum expected two int value columns")

  private def sumArrays(left: Array[Int], right: Array[Int], rows: Int): Long =
    var row = 0
    var sum = 0L
    while (row < rows) {
      sum += left(row).toLong * 31L + right(row).toLong
      row += 1
    }
    sum

  /** A null on either value drops the row. `SUM` does not add a null. */
  private def sumSparse(left: Int32Column, right: Int32Column, rows: Int): Long =
    var row = 0
    var sum = 0L
    while (row < rows) {
      if (left.isValid(row) && right.isValid(row)) then
        sum += left.values(row).toLong * 31L + right.values(row).toLong
      row += 1
    }
    sum

  /**
   * Every row in `[0, live)` is present. Spare bits past `live` are ignored. This is the same word
   * test as `Validity.livePresent`, which the harness cannot call.
   */
  private def present(column: Int32Column, live: Int): Boolean =
    if (live == 0) then true
    else
      val words = column.validity.words
      val full = live >>> 6
      var index = 0
      var dense = true
      while (index < full && dense) {
        if (words(index) != -1L) then dense = false
        index += 1
      }
      if (!dense) then false
      else
        val tail = live & 63
        if (tail == 0) then true
        else
          val mask = (1L << tail) - 1L
          (words(full) & mask) == mask

  private def intColumn(batch: ColumnBatch, ordinal: Int): Option[Int32Column] =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => Some(column)
      case _ => None

  /** Same sum as `rows.map { case (_, (left, right)) => left * 31L + right }.sum`. */
  private def collected(rows: Iterable[(Int, (Int, Int))]): Long =
    val iterator = rows.iterator
    var sum = 0L
    while (iterator.hasNext) {
      val pair = iterator.next()._2
      sum += pair._1.toLong * 31L + pair._2.toLong
    }
    sum
