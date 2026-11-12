package com.ewoodbury.scarlet.columnar

import ColumnKernel.{Int64Map, Int64Predicate}

/**
 * Filter and project over one `Int64` column.
 *
 * A filter is `Long => Boolean` and a project is `Long => Long`. `collect` applies
 * [[ColumnKernel.filterInt64]] and [[ColumnKernel.projectInt64]] in order, so each step publishes
 * its own column. The first step scans the morsel range. Every later step scans the batch the
 * previous step published, from row 0 through that batch's length. [[count]] walks the steps and
 * does not write that chain. [[sum]] does not write it either. It unrolls one step, every
 * filter/project pattern of length two to four, and five projects, the same arities as
 * [[Int32Pipeline]]. Those arms call [[ColumnKernel.Int64Predicate]] and [[ColumnKernel.Int64Map]]
 * directly. A longer chain stays on the walk. The planner still nests `collect` into pipelines of
 * at most four steps.
 *
 * Sum wraps at `Long`. Null slots are skipped. A filter drops a null and does not call the
 * predicate. A project leaves a null null and does not call the function. A map-only count is the
 * scanned length, including null slots, and still calls each mapper on the present rows. A failing
 * filter stops the row, so a later function does not run. [[sum]] and [[count]] scan
 * `[start, start + length)`. The batch entry uses the whole column.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object Int64Pipeline:

  enum Step:
    case Filter(predicate: Int64Predicate)
    case Project(mapper: Int64Map)

  /** Writes each step over the whole batch. The input batch is not mutated. */
  def apply(batch: ColumnBatch, steps: List[Step]): ColumnBatch =
    apply(Morsel(batch, 0, batch.length), steps)

  /**
   * The first step reads `[start, start + length)`. Each later step reads the batch that step
   * owns, whose length is the previous step's output length.
   */
  def apply(morsel: Morsel, steps: List[Step]): ColumnBatch =
    steps match
      case Nil => wholeMorsel(morsel)
      case first :: rest =>
        rest.foldLeft(applyRange(morsel, first)) { (current, step) =>
          applyStep(current, step)
        }

  private def wholeMorsel(morsel: Morsel): ColumnBatch =
    require(
      morsel.start == 0 && morsel.length == morsel.batch.length,
      "long pipeline has no step for a partial morsel",
    )
    morsel.batch

  private def applyRange(morsel: Morsel, step: Step): ColumnBatch =
    val batch = morsel.batch
    step match
      case Step.Filter(predicate) =>
        ColumnKernel.filterInt64(batch, 0, morsel.start, morsel.length, predicate)
      case Step.Project(mapper) =>
        ColumnKernel.projectInt64(batch, 0, morsel.start, morsel.length, mapper)

  private def applyStep(batch: ColumnBatch, step: Step): ColumnBatch =
    step match
      case Step.Filter(predicate) => ColumnKernel.filterInt64(batch, 0, predicate)
      case Step.Project(mapper) => ColumnKernel.projectInt64(batch, 0, mapper)

  /** Wrapping sum of the survivors. Does not allocate an output column. */
  def sum(batch: ColumnBatch, steps: List[Step]): Long =
    val column = single(batch)
    sum(column, 0, column.length, steps)

  private[scarlet] def sum(
      column: Int64Column,
      start: Int,
      length: Int,
      steps: List[Step],
  ): Long =
    sumSteps(column, start, length, steps)

  /**
   * Adds `process` over present rows of `[start, start + length)`. The total is a local `Long`.
   * `process` returns that row's contribution, or `0L` when a filter drops it, so no nested
   * function updates the total. A function that did would box the `Long` on every row. Adding zero
   * leaves the sum unchanged, including a kept zero. A null slot is not passed to `process`.
   */
  private inline def addPresent(
      column: Int64Column,
      start: Int,
      length: Int,
      inline process: Long => Long,
  ): Long =
    val end = start + length
    val values = column.values
    var total = 0L
    var row = start
    if (Validity.rangePresent(column.validity, start, length)) then
      while (row < end) {
        total += process(values(row))
        row += 1
      }
    else
      val words = column.validity.words
      while (row < end) {
        if (Validity.isSet(words, row)) then total += process(values(row))
        row += 1
      }
    total

  /** One match on the chain, then a direct call per step. Other lengths use [[fold]]. */
  private def sumSteps(
      column: Int64Column,
      start: Int,
      length: Int,
      steps: List[Step],
  ): Long =
    steps match
      case List(Step.Filter(first)) =>
        sumF(column, start, length, first)
      case List(Step.Project(first)) =>
        sumP(column, start, length, first)
      case List(Step.Filter(first), Step.Filter(second)) =>
        sumFF(column, start, length, first, second)
      case List(Step.Filter(first), Step.Project(second)) =>
        sumFP(column, start, length, first, second)
      case List(Step.Project(first), Step.Filter(second)) =>
        sumPF(column, start, length, first, second)
      case List(Step.Project(first), Step.Project(second)) =>
        sumPP(column, start, length, first, second)
      case List(Step.Filter(first), Step.Filter(second), Step.Filter(third)) =>
        sumFFF(column, start, length, first, second, third)
      case List(Step.Filter(first), Step.Filter(second), Step.Project(third)) =>
        sumFFP(column, start, length, first, second, third)
      case List(Step.Filter(first), Step.Project(second), Step.Filter(third)) =>
        sumFPF(column, start, length, first, second, third)
      case List(Step.Filter(first), Step.Project(second), Step.Project(third)) =>
        sumFPP(column, start, length, first, second, third)
      case List(Step.Project(first), Step.Filter(second), Step.Filter(third)) =>
        sumPFF(column, start, length, first, second, third)
      case List(Step.Project(first), Step.Filter(second), Step.Project(third)) =>
        sumPFP(column, start, length, first, second, third)
      case List(Step.Project(first), Step.Project(second), Step.Filter(third)) =>
        sumPPF(column, start, length, first, second, third)
      case List(Step.Project(first), Step.Project(second), Step.Project(third)) =>
        sumPPP(column, start, length, first, second, third)
      case List(
            Step.Filter(first),
            Step.Filter(second),
            Step.Filter(third),
            Step.Filter(fourth),
          ) =>
        sumFFFF(column, start, length, first, second, third, fourth)
      case List(
            Step.Filter(first),
            Step.Filter(second),
            Step.Filter(third),
            Step.Project(fourth),
          ) =>
        sumFFFP(column, start, length, first, second, third, fourth)
      case List(
            Step.Filter(first),
            Step.Filter(second),
            Step.Project(third),
            Step.Filter(fourth),
          ) =>
        sumFFPF(column, start, length, first, second, third, fourth)
      case List(
            Step.Filter(first),
            Step.Filter(second),
            Step.Project(third),
            Step.Project(fourth),
          ) =>
        sumFFPP(column, start, length, first, second, third, fourth)
      case List(
            Step.Filter(first),
            Step.Project(second),
            Step.Filter(third),
            Step.Filter(fourth),
          ) =>
        sumFPFF(column, start, length, first, second, third, fourth)
      case List(
            Step.Filter(first),
            Step.Project(second),
            Step.Filter(third),
            Step.Project(fourth),
          ) =>
        sumFPFP(column, start, length, first, second, third, fourth)
      case List(
            Step.Filter(first),
            Step.Project(second),
            Step.Project(third),
            Step.Filter(fourth),
          ) =>
        sumFPPF(column, start, length, first, second, third, fourth)
      case List(
            Step.Filter(first),
            Step.Project(second),
            Step.Project(third),
            Step.Project(fourth),
          ) =>
        sumFPPP(column, start, length, first, second, third, fourth)
      case List(
            Step.Project(first),
            Step.Filter(second),
            Step.Filter(third),
            Step.Filter(fourth),
          ) =>
        sumPFFF(column, start, length, first, second, third, fourth)
      case List(
            Step.Project(first),
            Step.Filter(second),
            Step.Filter(third),
            Step.Project(fourth),
          ) =>
        sumPFFP(column, start, length, first, second, third, fourth)
      case List(
            Step.Project(first),
            Step.Filter(second),
            Step.Project(third),
            Step.Filter(fourth),
          ) =>
        sumPFPF(column, start, length, first, second, third, fourth)
      case List(
            Step.Project(first),
            Step.Filter(second),
            Step.Project(third),
            Step.Project(fourth),
          ) =>
        sumPFPP(column, start, length, first, second, third, fourth)
      case List(
            Step.Project(first),
            Step.Project(second),
            Step.Filter(third),
            Step.Filter(fourth),
          ) =>
        sumPPFF(column, start, length, first, second, third, fourth)
      case List(
            Step.Project(first),
            Step.Project(second),
            Step.Filter(third),
            Step.Project(fourth),
          ) =>
        sumPPFP(column, start, length, first, second, third, fourth)
      case List(
            Step.Project(first),
            Step.Project(second),
            Step.Project(third),
            Step.Filter(fourth),
          ) =>
        sumPPPF(column, start, length, first, second, third, fourth)
      case List(
            Step.Project(first),
            Step.Project(second),
            Step.Project(third),
            Step.Project(fourth),
          ) =>
        sumPPPP(column, start, length, first, second, third, fourth)
      case List(
            Step.Project(first),
            Step.Project(second),
            Step.Project(third),
            Step.Project(fourth),
            Step.Project(fifth),
          ) =>
        sumPPPPP(column, start, length, first, second, third, fourth, fifth)
      case _ =>
        fold(column, start, length, steps, counting = false)

  private def sumF(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Predicate,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => if (first(value)) then value else 0L,
    )

  private def sumP(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Map,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => first(value),
    )

  private def sumFF(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Predicate,
      second: Int64Predicate,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => if (first(value) && second(value)) then value else 0L,
    )

  private def sumFP(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Predicate,
      second: Int64Map,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        if (first(value)) then second(value)
        else 0L
      },
    )

  private def sumPF(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Map,
      second: Int64Predicate,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        val mapped = first(value)
        if (second(mapped)) then mapped else 0L
      },
    )

  private def sumPP(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Map,
      second: Int64Map,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => second(first(value)),
    )

  private def sumFFF(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Predicate,
      second: Int64Predicate,
      third: Int64Predicate,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => if (first(value) && second(value) && third(value)) then value else 0L,
    )

  private def sumFFP(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Predicate,
      second: Int64Predicate,
      third: Int64Map,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        if (first(value) && second(value)) then third(value)
        else 0L
      },
    )

  private def sumFPF(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Predicate,
      second: Int64Map,
      third: Int64Predicate,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        if (first(value)) then
          val mapped = second(value)
          if (third(mapped)) then mapped else 0L
        else 0L
      },
    )

  private def sumFPP(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Predicate,
      second: Int64Map,
      third: Int64Map,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        if (first(value)) then third(second(value))
        else 0L
      },
    )

  private def sumPFF(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Map,
      second: Int64Predicate,
      third: Int64Predicate,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        val mapped = first(value)
        if (second(mapped) && third(mapped)) then mapped else 0L
      },
    )

  private def sumPFP(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Map,
      second: Int64Predicate,
      third: Int64Map,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        val mapped = first(value)
        if (second(mapped)) then third(mapped)
        else 0L
      },
    )

  private def sumPPF(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Map,
      second: Int64Map,
      third: Int64Predicate,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        val mapped = second(first(value))
        if (third(mapped)) then mapped else 0L
      },
    )

  private def sumPPP(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Map,
      second: Int64Map,
      third: Int64Map,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => third(second(first(value))),
    )

  private def sumFFFF(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Predicate,
      second: Int64Predicate,
      third: Int64Predicate,
      fourth: Int64Predicate,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value =>
        if (first(value) && second(value) && third(value) && fourth(value)) then value else 0L,
    )

  private def sumFFFP(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Predicate,
      second: Int64Predicate,
      third: Int64Predicate,
      fourth: Int64Map,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        if (first(value) && second(value) && third(value)) then fourth(value)
        else 0L
      },
    )

  private def sumFFPF(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Predicate,
      second: Int64Predicate,
      third: Int64Map,
      fourth: Int64Predicate,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        if (first(value) && second(value)) then
          val mapped = third(value)
          if (fourth(mapped)) then mapped else 0L
        else 0L
      },
    )

  private def sumFFPP(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Predicate,
      second: Int64Predicate,
      third: Int64Map,
      fourth: Int64Map,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        if (first(value) && second(value)) then fourth(third(value))
        else 0L
      },
    )

  private def sumFPFF(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Predicate,
      second: Int64Map,
      third: Int64Predicate,
      fourth: Int64Predicate,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        if (first(value)) then
          val mapped = second(value)
          if (third(mapped) && fourth(mapped)) then mapped else 0L
        else 0L
      },
    )

  private def sumFPFP(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Predicate,
      second: Int64Map,
      third: Int64Predicate,
      fourth: Int64Map,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        if (first(value)) then
          val mapped = second(value)
          if (third(mapped)) then fourth(mapped)
          else 0L
        else 0L
      },
    )

  private def sumFPPF(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Predicate,
      second: Int64Map,
      third: Int64Map,
      fourth: Int64Predicate,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        if (first(value)) then
          val mapped = third(second(value))
          if (fourth(mapped)) then mapped else 0L
        else 0L
      },
    )

  private def sumFPPP(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Predicate,
      second: Int64Map,
      third: Int64Map,
      fourth: Int64Map,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        if (first(value)) then fourth(third(second(value)))
        else 0L
      },
    )

  private def sumPFFF(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Map,
      second: Int64Predicate,
      third: Int64Predicate,
      fourth: Int64Predicate,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        val mapped = first(value)
        if (second(mapped) && third(mapped) && fourth(mapped)) then mapped else 0L
      },
    )

  private def sumPFFP(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Map,
      second: Int64Predicate,
      third: Int64Predicate,
      fourth: Int64Map,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        val mapped = first(value)
        if (second(mapped) && third(mapped)) then fourth(mapped)
        else 0L
      },
    )

  private def sumPFPF(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Map,
      second: Int64Predicate,
      third: Int64Map,
      fourth: Int64Predicate,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        val mapped = first(value)
        if (second(mapped)) then
          val next = third(mapped)
          if (fourth(next)) then next else 0L
        else 0L
      },
    )

  private def sumPFPP(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Map,
      second: Int64Predicate,
      third: Int64Map,
      fourth: Int64Map,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        val mapped = first(value)
        if (second(mapped)) then fourth(third(mapped))
        else 0L
      },
    )

  private def sumPPFF(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Map,
      second: Int64Map,
      third: Int64Predicate,
      fourth: Int64Predicate,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        val mapped = second(first(value))
        if (third(mapped) && fourth(mapped)) then mapped else 0L
      },
    )

  private def sumPPFP(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Map,
      second: Int64Map,
      third: Int64Predicate,
      fourth: Int64Map,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        val mapped = second(first(value))
        if (third(mapped)) then fourth(mapped)
        else 0L
      },
    )

  private def sumPPPF(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Map,
      second: Int64Map,
      third: Int64Map,
      fourth: Int64Predicate,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => {
        val mapped = third(second(first(value)))
        if (fourth(mapped)) then mapped else 0L
      },
    )

  private def sumPPPP(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Map,
      second: Int64Map,
      third: Int64Map,
      fourth: Int64Map,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => fourth(third(second(first(value)))),
    )

  private def sumPPPPP(
      column: Int64Column,
      start: Int,
      length: Int,
      first: Int64Map,
      second: Int64Map,
      third: Int64Map,
      fourth: Int64Map,
      fifth: Int64Map,
  ): Long =
    addPresent(
      column,
      start,
      length,
      value => fifth(fourth(third(second(first(value))))),
    )

  /**
   * Count without writing the chain. A filter counts survivors. A map-only chain returns the
   * scanned length after calling the mappers on present rows.
   */
  def count(batch: ColumnBatch, steps: List[Step]): Long =
    val column = single(batch)
    fold(column, 0, column.length, steps, counting = true)

  private[scarlet] def count(
      column: Int64Column,
      start: Int,
      length: Int,
      steps: List[Step],
  ): Long =
    fold(column, start, length, steps, counting = true)

  /**
   * `total` is a local `Long`. The step walk is written in each row loop so nothing closes over
   * it. A nested function that updated `total` would box that `Long` on every row. Rows are
   * absolute indexes in `[start, start + length)`.
   */
  private def fold(
      column: Int64Column,
      start: Int,
      length: Int,
      steps: List[Step],
      counting: Boolean,
  ): Long =
    val compiled = steps.toArray
    val compact = hasFilter(compiled)
    val end = start + length
    val values = column.values
    val words = column.validity.words
    val stepCount = compiled.length
    var total = 0L
    var row = start

    if (Validity.rangePresent(column.validity, start, length)) then
      while (row < end) {
        var kept = true
        var value = values(row)
        var index = 0
        while (index < stepCount && kept) {
          compiled(index) match
            case Step.Filter(predicate) => kept = predicate(value)
            case Step.Project(mapper) => value = mapper(value)
          index += 1
        }
        if (kept && !counting) then total += value
        else if (kept && compact) then total += 1L
        row += 1
      }
    else
      while (row < end) {
        if (Validity.isSet(words, row)) then
          var kept = true
          var value = values(row)
          var index = 0
          while (index < stepCount && kept) {
            compiled(index) match
              case Step.Filter(predicate) => kept = predicate(value)
              case Step.Project(mapper) => value = mapper(value)
            index += 1
          }
          if (kept && !counting) then total += value
          else if (kept && compact) then total += 1L
        row += 1
      }
    if (!counting || compact) then total else length.toLong

  private def hasFilter(steps: Array[Step]): Boolean =
    var index = 0
    var found = false
    while (index < steps.length && !found) {
      steps(index) match
        case Step.Filter(_) => found = true
        case Step.Project(_) => ()
      index += 1
    }
    found

  private[scarlet] def single(batch: ColumnBatch): Int64Column =
    require(
      batch.columns.length == 1,
      s"long pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    ColumnKernel.asInt64(ColumnKernel.columnAt(batch, 0), 0)
