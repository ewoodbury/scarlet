package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{
  ColumnBatch,
  ColumnKernel,
  Float64Pipeline,
  Int32Pipeline,
  Utf8Pipeline,
}

/** The 28 filter/project patterns for one element pipeline. The match is outside the row loop. */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Throw",
  ),
)
object ElementPipeline:

  def ints(batch: ColumnBatch, steps: List[ColumnarPlanner.ElementStep]): ColumnBatch =
    import ColumnarPlanner.ElementStep.*

    def pred(fn: Any): ColumnKernel.Int32Predicate =
      val function = fn.asInstanceOf[Int => Boolean]
      value => function(value)

    def proj(fn: Any): ColumnKernel.Int32Map =
      val function = fn.asInstanceOf[Int => Int]
      value => function(value)

    steps match
      case List(Filter(a), Filter(b)) =>
        Int32Pipeline.FF(batch, pred(a), pred(b))
      case List(Project(a), Filter(b)) =>
        Int32Pipeline.PF(batch, proj(a), pred(b))
      case List(Filter(a), Project(b)) =>
        Int32Pipeline.FP(batch, pred(a), proj(b))
      case List(Project(a), Project(b)) =>
        Int32Pipeline.PP(batch, proj(a), proj(b))
      case List(Filter(a), Filter(b), Filter(c)) =>
        Int32Pipeline.FFF(batch, pred(a), pred(b), pred(c))
      case List(Project(a), Filter(b), Filter(c)) =>
        Int32Pipeline.PFF(batch, proj(a), pred(b), pred(c))
      case List(Filter(a), Project(b), Filter(c)) =>
        Int32Pipeline.FPF(batch, pred(a), proj(b), pred(c))
      case List(Project(a), Project(b), Filter(c)) =>
        Int32Pipeline.PPF(batch, proj(a), proj(b), pred(c))
      case List(Filter(a), Filter(b), Project(c)) =>
        Int32Pipeline.FFP(batch, pred(a), pred(b), proj(c))
      case List(Project(a), Filter(b), Project(c)) =>
        Int32Pipeline.PFP(batch, proj(a), pred(b), proj(c))
      case List(Filter(a), Project(b), Project(c)) =>
        Int32Pipeline.FPP(batch, pred(a), proj(b), proj(c))
      case List(Project(a), Project(b), Project(c)) =>
        Int32Pipeline.PPP(batch, proj(a), proj(b), proj(c))
      case List(Filter(a), Filter(b), Filter(c), Filter(d)) =>
        Int32Pipeline.FFFF(batch, pred(a), pred(b), pred(c), pred(d))
      case List(Project(a), Filter(b), Filter(c), Filter(d)) =>
        Int32Pipeline.PFFF(batch, proj(a), pred(b), pred(c), pred(d))
      case List(Filter(a), Project(b), Filter(c), Filter(d)) =>
        Int32Pipeline.FPFF(batch, pred(a), proj(b), pred(c), pred(d))
      case List(Project(a), Project(b), Filter(c), Filter(d)) =>
        Int32Pipeline.PPFF(batch, proj(a), proj(b), pred(c), pred(d))
      case List(Filter(a), Filter(b), Project(c), Filter(d)) =>
        Int32Pipeline.FFPF(batch, pred(a), pred(b), proj(c), pred(d))
      case List(Project(a), Filter(b), Project(c), Filter(d)) =>
        Int32Pipeline.PFPF(batch, proj(a), pred(b), proj(c), pred(d))
      case List(Filter(a), Project(b), Project(c), Filter(d)) =>
        Int32Pipeline.FPPF(batch, pred(a), proj(b), proj(c), pred(d))
      case List(Project(a), Project(b), Project(c), Filter(d)) =>
        Int32Pipeline.PPPF(batch, proj(a), proj(b), proj(c), pred(d))
      case List(Filter(a), Filter(b), Filter(c), Project(d)) =>
        Int32Pipeline.FFFP(batch, pred(a), pred(b), pred(c), proj(d))
      case List(Project(a), Filter(b), Filter(c), Project(d)) =>
        Int32Pipeline.PFFP(batch, proj(a), pred(b), pred(c), proj(d))
      case List(Filter(a), Project(b), Filter(c), Project(d)) =>
        Int32Pipeline.FPFP(batch, pred(a), proj(b), pred(c), proj(d))
      case List(Project(a), Project(b), Filter(c), Project(d)) =>
        Int32Pipeline.PPFP(batch, proj(a), proj(b), pred(c), proj(d))
      case List(Filter(a), Filter(b), Project(c), Project(d)) =>
        Int32Pipeline.FFPP(batch, pred(a), pred(b), proj(c), proj(d))
      case List(Project(a), Filter(b), Project(c), Project(d)) =>
        Int32Pipeline.PFPP(batch, proj(a), pred(b), proj(c), proj(d))
      case List(Filter(a), Project(b), Project(c), Project(d)) =>
        Int32Pipeline.FPPP(batch, pred(a), proj(b), proj(c), proj(d))
      case List(Project(a), Project(b), Project(c), Project(d)) =>
        Int32Pipeline.PPPP(batch, proj(a), proj(b), proj(c), proj(d))
      case _ =>
        throw new IllegalStateException(s"unsupported ints pipeline of length ${steps.length}")

  def floats(batch: ColumnBatch, steps: List[ColumnarPlanner.ElementStep]): ColumnBatch =
    import ColumnarPlanner.ElementStep.*

    def pred(fn: Any): ColumnKernel.Float64Predicate =
      val function = fn.asInstanceOf[Double => Boolean]
      value => function(value)

    def proj(fn: Any): ColumnKernel.Float64Map =
      val function = fn.asInstanceOf[Double => Double]
      value => function(value)

    steps match
      case List(Filter(a), Filter(b)) =>
        Float64Pipeline.FF(batch, pred(a), pred(b))
      case List(Project(a), Filter(b)) =>
        Float64Pipeline.PF(batch, proj(a), pred(b))
      case List(Filter(a), Project(b)) =>
        Float64Pipeline.FP(batch, pred(a), proj(b))
      case List(Project(a), Project(b)) =>
        Float64Pipeline.PP(batch, proj(a), proj(b))
      case List(Filter(a), Filter(b), Filter(c)) =>
        Float64Pipeline.FFF(batch, pred(a), pred(b), pred(c))
      case List(Project(a), Filter(b), Filter(c)) =>
        Float64Pipeline.PFF(batch, proj(a), pred(b), pred(c))
      case List(Filter(a), Project(b), Filter(c)) =>
        Float64Pipeline.FPF(batch, pred(a), proj(b), pred(c))
      case List(Project(a), Project(b), Filter(c)) =>
        Float64Pipeline.PPF(batch, proj(a), proj(b), pred(c))
      case List(Filter(a), Filter(b), Project(c)) =>
        Float64Pipeline.FFP(batch, pred(a), pred(b), proj(c))
      case List(Project(a), Filter(b), Project(c)) =>
        Float64Pipeline.PFP(batch, proj(a), pred(b), proj(c))
      case List(Filter(a), Project(b), Project(c)) =>
        Float64Pipeline.FPP(batch, pred(a), proj(b), proj(c))
      case List(Project(a), Project(b), Project(c)) =>
        Float64Pipeline.PPP(batch, proj(a), proj(b), proj(c))
      case List(Filter(a), Filter(b), Filter(c), Filter(d)) =>
        Float64Pipeline.FFFF(batch, pred(a), pred(b), pred(c), pred(d))
      case List(Project(a), Filter(b), Filter(c), Filter(d)) =>
        Float64Pipeline.PFFF(batch, proj(a), pred(b), pred(c), pred(d))
      case List(Filter(a), Project(b), Filter(c), Filter(d)) =>
        Float64Pipeline.FPFF(batch, pred(a), proj(b), pred(c), pred(d))
      case List(Project(a), Project(b), Filter(c), Filter(d)) =>
        Float64Pipeline.PPFF(batch, proj(a), proj(b), pred(c), pred(d))
      case List(Filter(a), Filter(b), Project(c), Filter(d)) =>
        Float64Pipeline.FFPF(batch, pred(a), pred(b), proj(c), pred(d))
      case List(Project(a), Filter(b), Project(c), Filter(d)) =>
        Float64Pipeline.PFPF(batch, proj(a), pred(b), proj(c), pred(d))
      case List(Filter(a), Project(b), Project(c), Filter(d)) =>
        Float64Pipeline.FPPF(batch, pred(a), proj(b), proj(c), pred(d))
      case List(Project(a), Project(b), Project(c), Filter(d)) =>
        Float64Pipeline.PPPF(batch, proj(a), proj(b), proj(c), pred(d))
      case List(Filter(a), Filter(b), Filter(c), Project(d)) =>
        Float64Pipeline.FFFP(batch, pred(a), pred(b), pred(c), proj(d))
      case List(Project(a), Filter(b), Filter(c), Project(d)) =>
        Float64Pipeline.PFFP(batch, proj(a), pred(b), pred(c), proj(d))
      case List(Filter(a), Project(b), Filter(c), Project(d)) =>
        Float64Pipeline.FPFP(batch, pred(a), proj(b), pred(c), proj(d))
      case List(Project(a), Project(b), Filter(c), Project(d)) =>
        Float64Pipeline.PPFP(batch, proj(a), proj(b), pred(c), proj(d))
      case List(Filter(a), Filter(b), Project(c), Project(d)) =>
        Float64Pipeline.FFPP(batch, pred(a), pred(b), proj(c), proj(d))
      case List(Project(a), Filter(b), Project(c), Project(d)) =>
        Float64Pipeline.PFPP(batch, proj(a), pred(b), proj(c), proj(d))
      case List(Filter(a), Project(b), Project(c), Project(d)) =>
        Float64Pipeline.FPPP(batch, pred(a), proj(b), proj(c), proj(d))
      case List(Project(a), Project(b), Project(c), Project(d)) =>
        Float64Pipeline.PPPP(batch, proj(a), proj(b), proj(c), proj(d))
      case _ =>
        throw new IllegalStateException(s"unsupported floats pipeline of length ${steps.length}")

  def strings(batch: ColumnBatch, steps: List[ColumnarPlanner.ElementStep]): ColumnBatch =
    import ColumnarPlanner.ElementStep.*

    def pred(fn: Any): ColumnKernel.Utf8Predicate =
      val function = fn.asInstanceOf[String => Boolean]
      value => function(value)

    def proj(fn: Any): ColumnKernel.Utf8Map =
      val function = fn.asInstanceOf[String => Any]
      value => ColumnarPlanner.stringResult(function(value))

    steps match
      case List(Filter(a), Filter(b)) =>
        Utf8Pipeline.FF(batch, pred(a), pred(b))
      case List(Project(a), Filter(b)) =>
        Utf8Pipeline.PF(batch, proj(a), pred(b))
      case List(Filter(a), Project(b)) =>
        Utf8Pipeline.FP(batch, pred(a), proj(b))
      case List(Project(a), Project(b)) =>
        Utf8Pipeline.PP(batch, proj(a), proj(b))
      case List(Filter(a), Filter(b), Filter(c)) =>
        Utf8Pipeline.FFF(batch, pred(a), pred(b), pred(c))
      case List(Project(a), Filter(b), Filter(c)) =>
        Utf8Pipeline.PFF(batch, proj(a), pred(b), pred(c))
      case List(Filter(a), Project(b), Filter(c)) =>
        Utf8Pipeline.FPF(batch, pred(a), proj(b), pred(c))
      case List(Project(a), Project(b), Filter(c)) =>
        Utf8Pipeline.PPF(batch, proj(a), proj(b), pred(c))
      case List(Filter(a), Filter(b), Project(c)) =>
        Utf8Pipeline.FFP(batch, pred(a), pred(b), proj(c))
      case List(Project(a), Filter(b), Project(c)) =>
        Utf8Pipeline.PFP(batch, proj(a), pred(b), proj(c))
      case List(Filter(a), Project(b), Project(c)) =>
        Utf8Pipeline.FPP(batch, pred(a), proj(b), proj(c))
      case List(Project(a), Project(b), Project(c)) =>
        Utf8Pipeline.PPP(batch, proj(a), proj(b), proj(c))
      case List(Filter(a), Filter(b), Filter(c), Filter(d)) =>
        Utf8Pipeline.FFFF(batch, pred(a), pred(b), pred(c), pred(d))
      case List(Project(a), Filter(b), Filter(c), Filter(d)) =>
        Utf8Pipeline.PFFF(batch, proj(a), pred(b), pred(c), pred(d))
      case List(Filter(a), Project(b), Filter(c), Filter(d)) =>
        Utf8Pipeline.FPFF(batch, pred(a), proj(b), pred(c), pred(d))
      case List(Project(a), Project(b), Filter(c), Filter(d)) =>
        Utf8Pipeline.PPFF(batch, proj(a), proj(b), pred(c), pred(d))
      case List(Filter(a), Filter(b), Project(c), Filter(d)) =>
        Utf8Pipeline.FFPF(batch, pred(a), pred(b), proj(c), pred(d))
      case List(Project(a), Filter(b), Project(c), Filter(d)) =>
        Utf8Pipeline.PFPF(batch, proj(a), pred(b), proj(c), pred(d))
      case List(Filter(a), Project(b), Project(c), Filter(d)) =>
        Utf8Pipeline.FPPF(batch, pred(a), proj(b), proj(c), pred(d))
      case List(Project(a), Project(b), Project(c), Filter(d)) =>
        Utf8Pipeline.PPPF(batch, proj(a), proj(b), proj(c), pred(d))
      case List(Filter(a), Filter(b), Filter(c), Project(d)) =>
        Utf8Pipeline.FFFP(batch, pred(a), pred(b), pred(c), proj(d))
      case List(Project(a), Filter(b), Filter(c), Project(d)) =>
        Utf8Pipeline.PFFP(batch, proj(a), pred(b), pred(c), proj(d))
      case List(Filter(a), Project(b), Filter(c), Project(d)) =>
        Utf8Pipeline.FPFP(batch, pred(a), proj(b), pred(c), proj(d))
      case List(Project(a), Project(b), Filter(c), Project(d)) =>
        Utf8Pipeline.PPFP(batch, proj(a), proj(b), pred(c), proj(d))
      case List(Filter(a), Filter(b), Project(c), Project(d)) =>
        Utf8Pipeline.FFPP(batch, pred(a), pred(b), proj(c), proj(d))
      case List(Project(a), Filter(b), Project(c), Project(d)) =>
        Utf8Pipeline.PFPP(batch, proj(a), pred(b), proj(c), proj(d))
      case List(Filter(a), Project(b), Project(c), Project(d)) =>
        Utf8Pipeline.FPPP(batch, pred(a), proj(b), proj(c), proj(d))
      case List(Project(a), Project(b), Project(c), Project(d)) =>
        Utf8Pipeline.PPPP(batch, proj(a), proj(b), proj(c), proj(d))
      case _ =>
        throw new IllegalStateException(s"unsupported strings pipeline of length ${steps.length}")
