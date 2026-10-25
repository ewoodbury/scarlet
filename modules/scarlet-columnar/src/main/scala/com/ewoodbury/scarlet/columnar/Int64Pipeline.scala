package com.ewoodbury.scarlet.columnar

/**
 * Filter and project over one `Int64` column.
 *
 * A filter is `Long => Boolean` and a project is `Long => Long`. `collect` applies
 * [[ColumnKernel.filterInt64]] and [[ColumnKernel.projectInt64]] in order, so each step publishes
 * its own column. [[sum]] and [[count]] walk the same steps and do not write that chain. One walk
 * covers every length: one step, a fused run of two to four, and a longer chain. The planner still
 * nests `collect` into pipelines of at most four steps. Five and six steps have no separate
 * kernel; an action passes the whole list here.
 *
 * Sum wraps at `Long`. Null slots are skipped. A filter drops a null and does not call the
 * predicate. A project leaves a null null and does not call the function. A map-only count is the
 * batch length, including null slots, and still calls each mapper on the present rows. A failing
 * filter stops the row, so a later function does not run.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object Int64Pipeline:

  enum Step:
    case Filter(predicate: ColumnKernel.Int64Predicate)
    case Project(mapper: ColumnKernel.Int64Map)

  /** Writes each step. The input batch is not mutated. */
  def apply(batch: ColumnBatch, steps: List[Step]): ColumnBatch =
    steps.foldLeft(batch) { (current, step) =>
      step match
        case Step.Filter(predicate) => ColumnKernel.filterInt64(current, 0, predicate)
        case Step.Project(mapper) => ColumnKernel.projectInt64(current, 0, mapper)
    }

  /** Wrapping sum of the survivors. Does not allocate an output column. */
  def sum(batch: ColumnBatch, steps: List[Step]): Long =
    fold(batch, steps, counting = false)

  /**
   * Count without writing the chain. A filter counts survivors. A map-only chain returns
   * `batch.length` after calling the mappers on present rows.
   */
  def count(batch: ColumnBatch, steps: List[Step]): Long =
    fold(batch, steps, counting = true)

  /**
   * `total` is a local `Long`. The step walk is written in each row loop so nothing closes over
   * it. A nested function that updated `total` would box that `Long` on every row.
   */
  private def fold(batch: ColumnBatch, steps: List[Step], counting: Boolean): Long =
    val column = single(batch)
    val compiled = steps.toArray
    val compact = hasFilter(compiled)
    val live = column.length
    val values = column.values
    val words = column.validity.words
    val stepCount = compiled.length
    var total = 0L
    var row = 0

    if (Validity.livePresent(column.validity, live)) then
      while (row < live) {
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
      while (row < live) {
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
    if (!counting || compact) then total else live.toLong

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

  private def single(batch: ColumnBatch): Int64Column =
    require(
      batch.columns.length == 1,
      s"long pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    ColumnKernel.asInt64(ColumnKernel.columnAt(batch, 0), 0)
