package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

import com.ewoodbury.scarlet.columnar.*

/**
 * Sum a filter/project chain without writing its output column. [[Shape.of]] matches outside the
 * row loop. A chain longer than four stays in that fold: five int maps call
 * [[Int32Pipeline.sumPPPPP]], and any other longer int chain walks its steps. [[countLonger]]
 * counts two or more steps the same way. [[countMapped]] counts one trailing int project.
 * Survivors use [[Int32Pipeline.encoded]]. [[sumLongs]] and [[countLongs]] are the long arms of
 * that fold: one encoded walk, no output column, including a chain of five or six steps.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Throw",
  ),
)
private[execution] object ElementSum:

  def sumInts(morsel: Morsel, steps: List[ColumnarPlanner.ElementStep]): Long =
    val column = Int32Pipeline.singleInt(morsel.batch)
    val start = morsel.start
    val length = morsel.length

    def pred(fn: Any): ColumnKernel.Int32Predicate =
      val function = fn.asInstanceOf[Int => Boolean]
      value => function(value)

    Shape.of(steps) match
      case Some(Shape.FF(a, b)) =>
        Int32Pipeline.sumFF(column, start, length, pred(a), pred(b))
      case Some(Shape.PF(a, b)) =>
        Int32Pipeline.sumPF(column, start, length, proj(a), pred(b))
      case Some(Shape.FP(a, b)) =>
        Int32Pipeline.sumFP(column, start, length, pred(a), proj(b))
      case Some(Shape.PP(a, b)) =>
        Int32Pipeline.sumPP(column, start, length, proj(a), proj(b))
      case Some(Shape.FFF(a, b, c)) =>
        Int32Pipeline.sumFFF(column, start, length, pred(a), pred(b), pred(c))
      case Some(Shape.PFF(a, b, c)) =>
        Int32Pipeline.sumPFF(column, start, length, proj(a), pred(b), pred(c))
      case Some(Shape.FPF(a, b, c)) =>
        Int32Pipeline.sumFPF(column, start, length, pred(a), proj(b), pred(c))
      case Some(Shape.PPF(a, b, c)) =>
        Int32Pipeline.sumPPF(column, start, length, proj(a), proj(b), pred(c))
      case Some(Shape.FFP(a, b, c)) =>
        Int32Pipeline.sumFFP(column, start, length, pred(a), pred(b), proj(c))
      case Some(Shape.PFP(a, b, c)) =>
        Int32Pipeline.sumPFP(column, start, length, proj(a), pred(b), proj(c))
      case Some(Shape.FPP(a, b, c)) =>
        Int32Pipeline.sumFPP(column, start, length, pred(a), proj(b), proj(c))
      case Some(Shape.PPP(a, b, c)) =>
        Int32Pipeline.sumPPP(column, start, length, proj(a), proj(b), proj(c))
      case Some(Shape.FFFF(a, b, c, d)) =>
        Int32Pipeline.sumFFFF(column, start, length, pred(a), pred(b), pred(c), pred(d))
      case Some(Shape.PFFF(a, b, c, d)) =>
        Int32Pipeline.sumPFFF(column, start, length, proj(a), pred(b), pred(c), pred(d))
      case Some(Shape.FPFF(a, b, c, d)) =>
        Int32Pipeline.sumFPFF(column, start, length, pred(a), proj(b), pred(c), pred(d))
      case Some(Shape.PPFF(a, b, c, d)) =>
        Int32Pipeline.sumPPFF(column, start, length, proj(a), proj(b), pred(c), pred(d))
      case Some(Shape.FFPF(a, b, c, d)) =>
        Int32Pipeline.sumFFPF(column, start, length, pred(a), pred(b), proj(c), pred(d))
      case Some(Shape.PFPF(a, b, c, d)) =>
        Int32Pipeline.sumPFPF(column, start, length, proj(a), pred(b), proj(c), pred(d))
      case Some(Shape.FPPF(a, b, c, d)) =>
        Int32Pipeline.sumFPPF(column, start, length, pred(a), proj(b), proj(c), pred(d))
      case Some(Shape.PPPF(a, b, c, d)) =>
        Int32Pipeline.sumPPPF(column, start, length, proj(a), proj(b), proj(c), pred(d))
      case Some(Shape.FFFP(a, b, c, d)) =>
        Int32Pipeline.sumFFFP(column, start, length, pred(a), pred(b), pred(c), proj(d))
      case Some(Shape.PFFP(a, b, c, d)) =>
        Int32Pipeline.sumPFFP(column, start, length, proj(a), pred(b), pred(c), proj(d))
      case Some(Shape.FPFP(a, b, c, d)) =>
        Int32Pipeline.sumFPFP(column, start, length, pred(a), proj(b), pred(c), proj(d))
      case Some(Shape.PPFP(a, b, c, d)) =>
        Int32Pipeline.sumPPFP(column, start, length, proj(a), proj(b), pred(c), proj(d))
      case Some(Shape.FFPP(a, b, c, d)) =>
        Int32Pipeline.sumFFPP(column, start, length, pred(a), pred(b), proj(c), proj(d))
      case Some(Shape.PFPP(a, b, c, d)) =>
        Int32Pipeline.sumPFPP(column, start, length, proj(a), pred(b), proj(c), proj(d))
      case Some(Shape.FPPP(a, b, c, d)) =>
        Int32Pipeline.sumFPPP(column, start, length, pred(a), proj(b), proj(c), proj(d))
      case Some(Shape.PPPP(a, b, c, d)) =>
        Int32Pipeline.sumPPPP(column, start, length, proj(a), proj(b), proj(c), proj(d))
      case None =>
        Shape.unsupported("ints", steps.length)

  /**
   * Sum an int chain that `lower` split after four steps. Five projects use the specialized loop.
   * Any other chain, including a filter among the steps, walks the steps and does not write.
   */
  def sumLonger(morsel: Morsel, steps: List[ColumnarPlanner.ElementStep]): Long =
    val column = Int32Pipeline.singleInt(morsel.batch)
    val start = morsel.start
    val length = morsel.length
    import ColumnarPlanner.ElementStep.*

    steps match
      case List(Project(a), Project(b), Project(c), Project(d), Project(e)) =>
        Int32Pipeline.sumPPPPP(column, start, length, proj(a), proj(b), proj(c), proj(d), proj(e))
      case _ =>
        val stages = compile(steps)
        Int32Pipeline.sumEncoded(
          column,
          start,
          length,
          hasFilter(steps),
          value => runSteps(stages, value),
        )

  /**
   * Count without writing the chain, including two to four steps. A filter drops rows. A map-only
   * chain keeps null slots.
   */
  def countLonger(morsel: Morsel, steps: List[ColumnarPlanner.ElementStep]): Long =
    val column = Int32Pipeline.singleInt(morsel.batch)
    val start = morsel.start
    val length = morsel.length
    val stages = compile(steps)
    Int32Pipeline.countEncoded(
      column,
      start,
      length,
      hasFilter(steps),
      value => runSteps(stages, value),
    )

  /** Sum one int projection. The mapped column is not written. */
  def sumMapped(morsel: Morsel, mapper: Any): Long =
    val column = Int32Pipeline.singleInt(morsel.batch)
    val start = morsel.start
    val length = morsel.length
    Int32Pipeline.sumProjected(column, start, length, proj(mapper))

  /**
   * Count a single int projection. The mapped column is not written. Null slots stay in the count.
   */
  def countMapped(morsel: Morsel, mapper: Any): Long =
    val column = Int32Pipeline.singleInt(morsel.batch)
    val start = morsel.start
    val length = morsel.length
    val project = proj(mapper)
    Int32Pipeline.countEncoded(
      column,
      start,
      length,
      compact = false,
      value => Int32Pipeline.encoded(project(value)),
    )

  private def proj(fn: Any): ColumnKernel.Int32Map =
    val function = fn.asInstanceOf[Int => Int]
    value => function(value)

  private def hasFilter(steps: List[ColumnarPlanner.ElementStep]): Boolean =
    steps.exists {
      case ColumnarPlanner.ElementStep.Filter(_) => true
      case ColumnarPlanner.ElementStep.Project(_) => false
    }

  private def compile(steps: List[ColumnarPlanner.ElementStep]): List[Int => Long] =
    steps.map {
      case ColumnarPlanner.ElementStep.Filter(fn) =>
        val predicate = fn.asInstanceOf[Int => Boolean]
        (value: Int) => if (predicate(value)) then Int32Pipeline.encoded(value) else 0L
      case ColumnarPlanner.ElementStep.Project(fn) =>
        val mapper = fn.asInstanceOf[Int => Int]
        (value: Int) => Int32Pipeline.encoded(mapper(value))
    }

  @tailrec
  private def runSteps(stages: List[Int => Long], value: Int): Long =
    stages match
      case Nil => Int32Pipeline.encoded(value)
      case stage :: rest =>
        val encoded = stage(value)
        if ((encoded & 1L) == 0L) then 0L
        else runSteps(rest, (encoded >> 1).toInt)

  /** Wrapping long sum. Does not write the chain. Any length uses the same walk. */
  def sumLongs(morsel: Morsel, steps: List[ColumnarPlanner.ElementStep]): Long =
    val column = Int64Pipeline.single(morsel.batch)
    Int64Pipeline.sum(column, morsel.start, morsel.length, longSteps(steps))

  /** Long count without writing the chain. A map-only chain keeps null slots. */
  def countLongs(morsel: Morsel, steps: List[ColumnarPlanner.ElementStep]): Long =
    val column = Int64Pipeline.single(morsel.batch)
    Int64Pipeline.count(column, morsel.start, morsel.length, longSteps(steps))

  private def longSteps(steps: List[ColumnarPlanner.ElementStep]): List[Int64Pipeline.Step] =
    steps.map {
      case ColumnarPlanner.ElementStep.Filter(fn) =>
        // Named erasure boundary. A long filter is a runtime cast. A mistyped predicate throws
        // ClassCastException when the action runs, and the planner refuses that plan.
        val predicate = fn.asInstanceOf[Long => Boolean]
        Int64Pipeline.Step.Filter(value => predicate(value))
      case ColumnarPlanner.ElementStep.Project(fn) =>
        // Named erasure boundary. declaredElement required apply$mcJJ$sp before this plan lowered.
        val mapper = fn.asInstanceOf[Long => Long]
        Int64Pipeline.Step.Project(value => mapper(value))
    }

  def sumFloats(batch: ColumnBatch, steps: List[ColumnarPlanner.ElementStep]): Double =
    def pred(fn: Any): ColumnKernel.Float64Predicate =
      val function = fn.asInstanceOf[Double => Boolean]
      value => function(value)

    def proj(fn: Any): ColumnKernel.Float64Map =
      val function = fn.asInstanceOf[Double => Double]
      value => function(value)

    Shape.of(steps) match
      case Some(Shape.FF(a, b)) =>
        Float64Pipeline.sumFF(batch, pred(a), pred(b))
      case Some(Shape.PF(a, b)) =>
        Float64Pipeline.sumPF(batch, proj(a), pred(b))
      case Some(Shape.FP(a, b)) =>
        Float64Pipeline.sumFP(batch, pred(a), proj(b))
      case Some(Shape.PP(a, b)) =>
        Float64Pipeline.sumPP(batch, proj(a), proj(b))
      case Some(Shape.FFF(a, b, c)) =>
        Float64Pipeline.sumFFF(batch, pred(a), pred(b), pred(c))
      case Some(Shape.PFF(a, b, c)) =>
        Float64Pipeline.sumPFF(batch, proj(a), pred(b), pred(c))
      case Some(Shape.FPF(a, b, c)) =>
        Float64Pipeline.sumFPF(batch, pred(a), proj(b), pred(c))
      case Some(Shape.PPF(a, b, c)) =>
        Float64Pipeline.sumPPF(batch, proj(a), proj(b), pred(c))
      case Some(Shape.FFP(a, b, c)) =>
        Float64Pipeline.sumFFP(batch, pred(a), pred(b), proj(c))
      case Some(Shape.PFP(a, b, c)) =>
        Float64Pipeline.sumPFP(batch, proj(a), pred(b), proj(c))
      case Some(Shape.FPP(a, b, c)) =>
        Float64Pipeline.sumFPP(batch, pred(a), proj(b), proj(c))
      case Some(Shape.PPP(a, b, c)) =>
        Float64Pipeline.sumPPP(batch, proj(a), proj(b), proj(c))
      case Some(Shape.FFFF(a, b, c, d)) =>
        Float64Pipeline.sumFFFF(batch, pred(a), pred(b), pred(c), pred(d))
      case Some(Shape.PFFF(a, b, c, d)) =>
        Float64Pipeline.sumPFFF(batch, proj(a), pred(b), pred(c), pred(d))
      case Some(Shape.FPFF(a, b, c, d)) =>
        Float64Pipeline.sumFPFF(batch, pred(a), proj(b), pred(c), pred(d))
      case Some(Shape.PPFF(a, b, c, d)) =>
        Float64Pipeline.sumPPFF(batch, proj(a), proj(b), pred(c), pred(d))
      case Some(Shape.FFPF(a, b, c, d)) =>
        Float64Pipeline.sumFFPF(batch, pred(a), pred(b), proj(c), pred(d))
      case Some(Shape.PFPF(a, b, c, d)) =>
        Float64Pipeline.sumPFPF(batch, proj(a), pred(b), proj(c), pred(d))
      case Some(Shape.FPPF(a, b, c, d)) =>
        Float64Pipeline.sumFPPF(batch, pred(a), proj(b), proj(c), pred(d))
      case Some(Shape.PPPF(a, b, c, d)) =>
        Float64Pipeline.sumPPPF(batch, proj(a), proj(b), proj(c), pred(d))
      case Some(Shape.FFFP(a, b, c, d)) =>
        Float64Pipeline.sumFFFP(batch, pred(a), pred(b), pred(c), proj(d))
      case Some(Shape.PFFP(a, b, c, d)) =>
        Float64Pipeline.sumPFFP(batch, proj(a), pred(b), pred(c), proj(d))
      case Some(Shape.FPFP(a, b, c, d)) =>
        Float64Pipeline.sumFPFP(batch, pred(a), proj(b), pred(c), proj(d))
      case Some(Shape.PPFP(a, b, c, d)) =>
        Float64Pipeline.sumPPFP(batch, proj(a), proj(b), pred(c), proj(d))
      case Some(Shape.FFPP(a, b, c, d)) =>
        Float64Pipeline.sumFFPP(batch, pred(a), pred(b), proj(c), proj(d))
      case Some(Shape.PFPP(a, b, c, d)) =>
        Float64Pipeline.sumPFPP(batch, proj(a), pred(b), proj(c), proj(d))
      case Some(Shape.FPPP(a, b, c, d)) =>
        Float64Pipeline.sumFPPP(batch, pred(a), proj(b), proj(c), proj(d))
      case Some(Shape.PPPP(a, b, c, d)) =>
        Float64Pipeline.sumPPPP(batch, proj(a), proj(b), proj(c), proj(d))
      case None =>
        Shape.unsupported("floats", steps.length)
