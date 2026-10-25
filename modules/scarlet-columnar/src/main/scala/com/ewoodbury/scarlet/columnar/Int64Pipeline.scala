package com.ewoodbury.scarlet.columnar

/**
 * Filter and project over one `Int64` column.
 *
 * A filter is `Long => Boolean` and a project is `Long => Long`. `collect` applies
 * [[ColumnKernel.filterInt64]] and [[ColumnKernel.projectInt64]] in order, so each step publishes
 * its own column. The first step scans the morsel range. Every later step scans the batch the
 * previous step published, from row 0 through that batch's length. [[sum]] and [[count]] walk the
 * same steps and do not write that chain. One walk covers every length: one step, a run of two to
 * four, and a longer chain. The planner still nests `collect` into pipelines of at most four
 * steps. Five and six steps have no separate kernel; an action passes the whole list here.
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
    case Filter(predicate: ColumnKernel.Int64Predicate)
    case Project(mapper: ColumnKernel.Int64Map)

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
    fold(column, 0, column.length, steps, counting = false)

  private[scarlet] def sum(
      column: Int64Column,
      start: Int,
      length: Int,
      steps: List[Step],
  ): Long =
    fold(column, start, length, steps, counting = false)

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
