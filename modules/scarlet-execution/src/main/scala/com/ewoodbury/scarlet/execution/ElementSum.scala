package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{ColumnBatch, ColumnKernel, Float64Pipeline, Int32Pipeline}

/**
 * Sum a filter/project chain without writing its output column. The match is outside the row loop.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Throw",
  ),
)
object ElementSum:

  def sumInts(batch: ColumnBatch, steps: List[ColumnarPlanner.ElementStep]): Long =
    import ColumnarPlanner.ElementStep.*

    def pred(fn: Any): ColumnKernel.Int32Predicate =
      val function = fn.asInstanceOf[Int => Boolean]
      value => function(value)

    def proj(fn: Any): ColumnKernel.Int32Map =
      val function = fn.asInstanceOf[Int => Int]
      value => function(value)

    steps match
      case List(Filter(a), Filter(b)) =>
        Int32Pipeline.sumFF(batch, pred(a), pred(b))
      case List(Project(a), Filter(b)) =>
        Int32Pipeline.sumPF(batch, proj(a), pred(b))
      case List(Filter(a), Project(b)) =>
        Int32Pipeline.sumFP(batch, pred(a), proj(b))
      case List(Project(a), Project(b)) =>
        Int32Pipeline.sumPP(batch, proj(a), proj(b))
      case List(Filter(a), Filter(b), Filter(c)) =>
        Int32Pipeline.sumFFF(batch, pred(a), pred(b), pred(c))
      case List(Project(a), Filter(b), Filter(c)) =>
        Int32Pipeline.sumPFF(batch, proj(a), pred(b), pred(c))
      case List(Filter(a), Project(b), Filter(c)) =>
        Int32Pipeline.sumFPF(batch, pred(a), proj(b), pred(c))
      case List(Project(a), Project(b), Filter(c)) =>
        Int32Pipeline.sumPPF(batch, proj(a), proj(b), pred(c))
      case List(Filter(a), Filter(b), Project(c)) =>
        Int32Pipeline.sumFFP(batch, pred(a), pred(b), proj(c))
      case List(Project(a), Filter(b), Project(c)) =>
        Int32Pipeline.sumPFP(batch, proj(a), pred(b), proj(c))
      case List(Filter(a), Project(b), Project(c)) =>
        Int32Pipeline.sumFPP(batch, pred(a), proj(b), proj(c))
      case List(Project(a), Project(b), Project(c)) =>
        Int32Pipeline.sumPPP(batch, proj(a), proj(b), proj(c))
      case List(Filter(a), Filter(b), Filter(c), Filter(d)) =>
        Int32Pipeline.sumFFFF(batch, pred(a), pred(b), pred(c), pred(d))
      case List(Project(a), Filter(b), Filter(c), Filter(d)) =>
        Int32Pipeline.sumPFFF(batch, proj(a), pred(b), pred(c), pred(d))
      case List(Filter(a), Project(b), Filter(c), Filter(d)) =>
        Int32Pipeline.sumFPFF(batch, pred(a), proj(b), pred(c), pred(d))
      case List(Project(a), Project(b), Filter(c), Filter(d)) =>
        Int32Pipeline.sumPPFF(batch, proj(a), proj(b), pred(c), pred(d))
      case List(Filter(a), Filter(b), Project(c), Filter(d)) =>
        Int32Pipeline.sumFFPF(batch, pred(a), pred(b), proj(c), pred(d))
      case List(Project(a), Filter(b), Project(c), Filter(d)) =>
        Int32Pipeline.sumPFPF(batch, proj(a), pred(b), proj(c), pred(d))
      case List(Filter(a), Project(b), Project(c), Filter(d)) =>
        Int32Pipeline.sumFPPF(batch, pred(a), proj(b), proj(c), pred(d))
      case List(Project(a), Project(b), Project(c), Filter(d)) =>
        Int32Pipeline.sumPPPF(batch, proj(a), proj(b), proj(c), pred(d))
      case List(Filter(a), Filter(b), Filter(c), Project(d)) =>
        Int32Pipeline.sumFFFP(batch, pred(a), pred(b), pred(c), proj(d))
      case List(Project(a), Filter(b), Filter(c), Project(d)) =>
        Int32Pipeline.sumPFFP(batch, proj(a), pred(b), pred(c), proj(d))
      case List(Filter(a), Project(b), Filter(c), Project(d)) =>
        Int32Pipeline.sumFPFP(batch, pred(a), proj(b), pred(c), proj(d))
      case List(Project(a), Project(b), Filter(c), Project(d)) =>
        Int32Pipeline.sumPPFP(batch, proj(a), proj(b), pred(c), proj(d))
      case List(Filter(a), Filter(b), Project(c), Project(d)) =>
        Int32Pipeline.sumFFPP(batch, pred(a), pred(b), proj(c), proj(d))
      case List(Project(a), Filter(b), Project(c), Project(d)) =>
        Int32Pipeline.sumPFPP(batch, proj(a), pred(b), proj(c), proj(d))
      case List(Filter(a), Project(b), Project(c), Project(d)) =>
        Int32Pipeline.sumFPPP(batch, pred(a), proj(b), proj(c), proj(d))
      case List(Project(a), Project(b), Project(c), Project(d)) =>
        Int32Pipeline.sumPPPP(batch, proj(a), proj(b), proj(c), proj(d))
      case _ =>
        throw new IllegalStateException(s"unsupported ints pipeline of length ${steps.length}")

  def sumFloats(batch: ColumnBatch, steps: List[ColumnarPlanner.ElementStep]): Double =
    import ColumnarPlanner.ElementStep.*

    def pred(fn: Any): ColumnKernel.Float64Predicate =
      val function = fn.asInstanceOf[Double => Boolean]
      value => function(value)

    def proj(fn: Any): ColumnKernel.Float64Map =
      val function = fn.asInstanceOf[Double => Double]
      value => function(value)

    steps match
      case List(Filter(a), Filter(b)) =>
        Float64Pipeline.sumFF(batch, pred(a), pred(b))
      case List(Project(a), Filter(b)) =>
        Float64Pipeline.sumPF(batch, proj(a), pred(b))
      case List(Filter(a), Project(b)) =>
        Float64Pipeline.sumFP(batch, pred(a), proj(b))
      case List(Project(a), Project(b)) =>
        Float64Pipeline.sumPP(batch, proj(a), proj(b))
      case List(Filter(a), Filter(b), Filter(c)) =>
        Float64Pipeline.sumFFF(batch, pred(a), pred(b), pred(c))
      case List(Project(a), Filter(b), Filter(c)) =>
        Float64Pipeline.sumPFF(batch, proj(a), pred(b), pred(c))
      case List(Filter(a), Project(b), Filter(c)) =>
        Float64Pipeline.sumFPF(batch, pred(a), proj(b), pred(c))
      case List(Project(a), Project(b), Filter(c)) =>
        Float64Pipeline.sumPPF(batch, proj(a), proj(b), pred(c))
      case List(Filter(a), Filter(b), Project(c)) =>
        Float64Pipeline.sumFFP(batch, pred(a), pred(b), proj(c))
      case List(Project(a), Filter(b), Project(c)) =>
        Float64Pipeline.sumPFP(batch, proj(a), pred(b), proj(c))
      case List(Filter(a), Project(b), Project(c)) =>
        Float64Pipeline.sumFPP(batch, pred(a), proj(b), proj(c))
      case List(Project(a), Project(b), Project(c)) =>
        Float64Pipeline.sumPPP(batch, proj(a), proj(b), proj(c))
      case List(Filter(a), Filter(b), Filter(c), Filter(d)) =>
        Float64Pipeline.sumFFFF(batch, pred(a), pred(b), pred(c), pred(d))
      case List(Project(a), Filter(b), Filter(c), Filter(d)) =>
        Float64Pipeline.sumPFFF(batch, proj(a), pred(b), pred(c), pred(d))
      case List(Filter(a), Project(b), Filter(c), Filter(d)) =>
        Float64Pipeline.sumFPFF(batch, pred(a), proj(b), pred(c), pred(d))
      case List(Project(a), Project(b), Filter(c), Filter(d)) =>
        Float64Pipeline.sumPPFF(batch, proj(a), proj(b), pred(c), pred(d))
      case List(Filter(a), Filter(b), Project(c), Filter(d)) =>
        Float64Pipeline.sumFFPF(batch, pred(a), pred(b), proj(c), pred(d))
      case List(Project(a), Filter(b), Project(c), Filter(d)) =>
        Float64Pipeline.sumPFPF(batch, proj(a), pred(b), proj(c), pred(d))
      case List(Filter(a), Project(b), Project(c), Filter(d)) =>
        Float64Pipeline.sumFPPF(batch, pred(a), proj(b), proj(c), pred(d))
      case List(Project(a), Project(b), Project(c), Filter(d)) =>
        Float64Pipeline.sumPPPF(batch, proj(a), proj(b), proj(c), pred(d))
      case List(Filter(a), Filter(b), Filter(c), Project(d)) =>
        Float64Pipeline.sumFFFP(batch, pred(a), pred(b), pred(c), proj(d))
      case List(Project(a), Filter(b), Filter(c), Project(d)) =>
        Float64Pipeline.sumPFFP(batch, proj(a), pred(b), pred(c), proj(d))
      case List(Filter(a), Project(b), Filter(c), Project(d)) =>
        Float64Pipeline.sumFPFP(batch, pred(a), proj(b), pred(c), proj(d))
      case List(Project(a), Project(b), Filter(c), Project(d)) =>
        Float64Pipeline.sumPPFP(batch, proj(a), proj(b), pred(c), proj(d))
      case List(Filter(a), Filter(b), Project(c), Project(d)) =>
        Float64Pipeline.sumFFPP(batch, pred(a), pred(b), proj(c), proj(d))
      case List(Project(a), Filter(b), Project(c), Project(d)) =>
        Float64Pipeline.sumPFPP(batch, proj(a), pred(b), proj(c), proj(d))
      case List(Filter(a), Project(b), Project(c), Project(d)) =>
        Float64Pipeline.sumFPPP(batch, pred(a), proj(b), proj(c), proj(d))
      case List(Project(a), Project(b), Project(c), Project(d)) =>
        Float64Pipeline.sumPPPP(batch, proj(a), proj(b), proj(c), proj(d))
      case _ =>
        throw new IllegalStateException(s"unsupported floats pipeline of length ${steps.length}")
