package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*

/** The 28 filter/project patterns for one element pipeline. The match is outside the row loop. */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Throw",
  ),
)
object ElementPipeline:

  /** Scans the morsel. The output batch covers only that range. */
  def ints(morsel: Morsel, steps: List[ColumnarPlanner.ElementStep]): ColumnBatch =
    import ColumnarPlanner.ElementStep.*

    def pred(fn: Any): ColumnKernel.Int32Predicate =
      val function = fn.asInstanceOf[Int => Boolean]
      value => function(value)

    def proj(fn: Any): ColumnKernel.Int32Map =
      val function = fn.asInstanceOf[Int => Int]
      value => function(value)

    steps match
      case List(Filter(a), Filter(b)) =>
        Int32Pipeline.FF(morsel, pred(a), pred(b))
      case List(Project(a), Filter(b)) =>
        Int32Pipeline.PF(morsel, proj(a), pred(b))
      case List(Filter(a), Project(b)) =>
        Int32Pipeline.FP(morsel, pred(a), proj(b))
      case List(Project(a), Project(b)) =>
        Int32Pipeline.PP(morsel, proj(a), proj(b))
      case List(Filter(a), Filter(b), Filter(c)) =>
        Int32Pipeline.FFF(morsel, pred(a), pred(b), pred(c))
      case List(Project(a), Filter(b), Filter(c)) =>
        Int32Pipeline.PFF(morsel, proj(a), pred(b), pred(c))
      case List(Filter(a), Project(b), Filter(c)) =>
        Int32Pipeline.FPF(morsel, pred(a), proj(b), pred(c))
      case List(Project(a), Project(b), Filter(c)) =>
        Int32Pipeline.PPF(morsel, proj(a), proj(b), pred(c))
      case List(Filter(a), Filter(b), Project(c)) =>
        Int32Pipeline.FFP(morsel, pred(a), pred(b), proj(c))
      case List(Project(a), Filter(b), Project(c)) =>
        Int32Pipeline.PFP(morsel, proj(a), pred(b), proj(c))
      case List(Filter(a), Project(b), Project(c)) =>
        Int32Pipeline.FPP(morsel, pred(a), proj(b), proj(c))
      case List(Project(a), Project(b), Project(c)) =>
        Int32Pipeline.PPP(morsel, proj(a), proj(b), proj(c))
      case List(Filter(a), Filter(b), Filter(c), Filter(d)) =>
        Int32Pipeline.FFFF(morsel, pred(a), pred(b), pred(c), pred(d))
      case List(Project(a), Filter(b), Filter(c), Filter(d)) =>
        Int32Pipeline.PFFF(morsel, proj(a), pred(b), pred(c), pred(d))
      case List(Filter(a), Project(b), Filter(c), Filter(d)) =>
        Int32Pipeline.FPFF(morsel, pred(a), proj(b), pred(c), pred(d))
      case List(Project(a), Project(b), Filter(c), Filter(d)) =>
        Int32Pipeline.PPFF(morsel, proj(a), proj(b), pred(c), pred(d))
      case List(Filter(a), Filter(b), Project(c), Filter(d)) =>
        Int32Pipeline.FFPF(morsel, pred(a), pred(b), proj(c), pred(d))
      case List(Project(a), Filter(b), Project(c), Filter(d)) =>
        Int32Pipeline.PFPF(morsel, proj(a), pred(b), proj(c), pred(d))
      case List(Filter(a), Project(b), Project(c), Filter(d)) =>
        Int32Pipeline.FPPF(morsel, pred(a), proj(b), proj(c), pred(d))
      case List(Project(a), Project(b), Project(c), Filter(d)) =>
        Int32Pipeline.PPPF(morsel, proj(a), proj(b), proj(c), pred(d))
      case List(Filter(a), Filter(b), Filter(c), Project(d)) =>
        Int32Pipeline.FFFP(morsel, pred(a), pred(b), pred(c), proj(d))
      case List(Project(a), Filter(b), Filter(c), Project(d)) =>
        Int32Pipeline.PFFP(morsel, proj(a), pred(b), pred(c), proj(d))
      case List(Filter(a), Project(b), Filter(c), Project(d)) =>
        Int32Pipeline.FPFP(morsel, pred(a), proj(b), pred(c), proj(d))
      case List(Project(a), Project(b), Filter(c), Project(d)) =>
        Int32Pipeline.PPFP(morsel, proj(a), proj(b), pred(c), proj(d))
      case List(Filter(a), Filter(b), Project(c), Project(d)) =>
        Int32Pipeline.FFPP(morsel, pred(a), pred(b), proj(c), proj(d))
      case List(Project(a), Filter(b), Project(c), Project(d)) =>
        Int32Pipeline.PFPP(morsel, proj(a), pred(b), proj(c), proj(d))
      case List(Filter(a), Project(b), Project(c), Project(d)) =>
        Int32Pipeline.FPPP(morsel, pred(a), proj(b), proj(c), proj(d))
      case List(Project(a), Project(b), Project(c), Project(d)) =>
        Int32Pipeline.PPPP(morsel, proj(a), proj(b), proj(c), proj(d))
      case _ =>
        throw new IllegalStateException(s"unsupported ints pipeline of length ${steps.length}")

  /** Scans the morsel. The first step reads that range. Later steps read the owned batch. */
  def longs(morsel: Morsel, steps: List[ColumnarPlanner.ElementStep]): ColumnBatch =
    import ColumnarPlanner.ElementStep.*

    Int64Pipeline(
      morsel,
      steps.map {
        case Filter(fn) =>
          // Named erasure boundary. A long filter is a runtime cast.
          val predicate = fn.asInstanceOf[Long => Boolean]
          Int64Pipeline.Step.Filter(value => predicate(value))
        case Project(fn) =>
          // Named erasure boundary. declaredElement required apply$mcJJ$sp before lowering.
          val mapper = fn.asInstanceOf[Long => Long]
          Int64Pipeline.Step.Project(value => mapper(value))
      },
    )

  /** One fused scan of the morsel. The output batch covers only that range. */
  def floats(morsel: Morsel, steps: List[ColumnarPlanner.ElementStep]): ColumnBatch =
    import ColumnarPlanner.ElementStep.*

    def pred(fn: Any): ColumnKernel.Float64Predicate =
      val function = fn.asInstanceOf[Double => Boolean]
      value => function(value)

    def proj(fn: Any): ColumnKernel.Float64Map =
      val function = fn.asInstanceOf[Double => Double]
      value => function(value)

    steps match
      case List(Filter(a), Filter(b)) =>
        Float64Pipeline.FF(morsel, pred(a), pred(b))
      case List(Project(a), Filter(b)) =>
        Float64Pipeline.PF(morsel, proj(a), pred(b))
      case List(Filter(a), Project(b)) =>
        Float64Pipeline.FP(morsel, pred(a), proj(b))
      case List(Project(a), Project(b)) =>
        Float64Pipeline.PP(morsel, proj(a), proj(b))
      case List(Filter(a), Filter(b), Filter(c)) =>
        Float64Pipeline.FFF(morsel, pred(a), pred(b), pred(c))
      case List(Project(a), Filter(b), Filter(c)) =>
        Float64Pipeline.PFF(morsel, proj(a), pred(b), pred(c))
      case List(Filter(a), Project(b), Filter(c)) =>
        Float64Pipeline.FPF(morsel, pred(a), proj(b), pred(c))
      case List(Project(a), Project(b), Filter(c)) =>
        Float64Pipeline.PPF(morsel, proj(a), proj(b), pred(c))
      case List(Filter(a), Filter(b), Project(c)) =>
        Float64Pipeline.FFP(morsel, pred(a), pred(b), proj(c))
      case List(Project(a), Filter(b), Project(c)) =>
        Float64Pipeline.PFP(morsel, proj(a), pred(b), proj(c))
      case List(Filter(a), Project(b), Project(c)) =>
        Float64Pipeline.FPP(morsel, pred(a), proj(b), proj(c))
      case List(Project(a), Project(b), Project(c)) =>
        Float64Pipeline.PPP(morsel, proj(a), proj(b), proj(c))
      case List(Filter(a), Filter(b), Filter(c), Filter(d)) =>
        Float64Pipeline.FFFF(morsel, pred(a), pred(b), pred(c), pred(d))
      case List(Project(a), Filter(b), Filter(c), Filter(d)) =>
        Float64Pipeline.PFFF(morsel, proj(a), pred(b), pred(c), pred(d))
      case List(Filter(a), Project(b), Filter(c), Filter(d)) =>
        Float64Pipeline.FPFF(morsel, pred(a), proj(b), pred(c), pred(d))
      case List(Project(a), Project(b), Filter(c), Filter(d)) =>
        Float64Pipeline.PPFF(morsel, proj(a), proj(b), pred(c), pred(d))
      case List(Filter(a), Filter(b), Project(c), Filter(d)) =>
        Float64Pipeline.FFPF(morsel, pred(a), pred(b), proj(c), pred(d))
      case List(Project(a), Filter(b), Project(c), Filter(d)) =>
        Float64Pipeline.PFPF(morsel, proj(a), pred(b), proj(c), pred(d))
      case List(Filter(a), Project(b), Project(c), Filter(d)) =>
        Float64Pipeline.FPPF(morsel, pred(a), proj(b), proj(c), pred(d))
      case List(Project(a), Project(b), Project(c), Filter(d)) =>
        Float64Pipeline.PPPF(morsel, proj(a), proj(b), proj(c), pred(d))
      case List(Filter(a), Filter(b), Filter(c), Project(d)) =>
        Float64Pipeline.FFFP(morsel, pred(a), pred(b), pred(c), proj(d))
      case List(Project(a), Filter(b), Filter(c), Project(d)) =>
        Float64Pipeline.PFFP(morsel, proj(a), pred(b), pred(c), proj(d))
      case List(Filter(a), Project(b), Filter(c), Project(d)) =>
        Float64Pipeline.FPFP(morsel, pred(a), proj(b), pred(c), proj(d))
      case List(Project(a), Project(b), Filter(c), Project(d)) =>
        Float64Pipeline.PPFP(morsel, proj(a), proj(b), pred(c), proj(d))
      case List(Filter(a), Filter(b), Project(c), Project(d)) =>
        Float64Pipeline.FFPP(morsel, pred(a), pred(b), proj(c), proj(d))
      case List(Project(a), Filter(b), Project(c), Project(d)) =>
        Float64Pipeline.PFPP(morsel, proj(a), pred(b), proj(c), proj(d))
      case List(Filter(a), Project(b), Project(c), Project(d)) =>
        Float64Pipeline.FPPP(morsel, pred(a), proj(b), proj(c), proj(d))
      case List(Project(a), Project(b), Project(c), Project(d)) =>
        Float64Pipeline.PPPP(morsel, proj(a), proj(b), proj(c), proj(d))
      case _ =>
        throw new IllegalStateException(s"unsupported floats pipeline of length ${steps.length}")

  /**
   * One fused scan of the morsel. A project runs on empty cells and builds a new dictionary. A
   * filter-only chain shares the input dictionary.
   */
  def strings(morsel: Morsel, steps: List[ColumnarPlanner.ElementStep]): ColumnBatch =
    import ColumnarPlanner.ElementStep.*

    def pred(fn: Any): ColumnKernel.Utf8Predicate =
      val function = fn.asInstanceOf[String => Boolean]
      value => function(value)

    def proj(fn: Any): ColumnKernel.Utf8Map =
      val function = fn.asInstanceOf[String => Any]
      value => ColumnarPlanner.stringResult(function(value))

    steps match
      case List(Filter(a), Filter(b)) =>
        Utf8Pipeline.FF(morsel, pred(a), pred(b))
      case List(Project(a), Filter(b)) =>
        Utf8Pipeline.PF(morsel, proj(a), pred(b))
      case List(Filter(a), Project(b)) =>
        Utf8Pipeline.FP(morsel, pred(a), proj(b))
      case List(Project(a), Project(b)) =>
        Utf8Pipeline.PP(morsel, proj(a), proj(b))
      case List(Filter(a), Filter(b), Filter(c)) =>
        Utf8Pipeline.FFF(morsel, pred(a), pred(b), pred(c))
      case List(Project(a), Filter(b), Filter(c)) =>
        Utf8Pipeline.PFF(morsel, proj(a), pred(b), pred(c))
      case List(Filter(a), Project(b), Filter(c)) =>
        Utf8Pipeline.FPF(morsel, pred(a), proj(b), pred(c))
      case List(Project(a), Project(b), Filter(c)) =>
        Utf8Pipeline.PPF(morsel, proj(a), proj(b), pred(c))
      case List(Filter(a), Filter(b), Project(c)) =>
        Utf8Pipeline.FFP(morsel, pred(a), pred(b), proj(c))
      case List(Project(a), Filter(b), Project(c)) =>
        Utf8Pipeline.PFP(morsel, proj(a), pred(b), proj(c))
      case List(Filter(a), Project(b), Project(c)) =>
        Utf8Pipeline.FPP(morsel, pred(a), proj(b), proj(c))
      case List(Project(a), Project(b), Project(c)) =>
        Utf8Pipeline.PPP(morsel, proj(a), proj(b), proj(c))
      case List(Filter(a), Filter(b), Filter(c), Filter(d)) =>
        Utf8Pipeline.FFFF(morsel, pred(a), pred(b), pred(c), pred(d))
      case List(Project(a), Filter(b), Filter(c), Filter(d)) =>
        Utf8Pipeline.PFFF(morsel, proj(a), pred(b), pred(c), pred(d))
      case List(Filter(a), Project(b), Filter(c), Filter(d)) =>
        Utf8Pipeline.FPFF(morsel, pred(a), proj(b), pred(c), pred(d))
      case List(Project(a), Project(b), Filter(c), Filter(d)) =>
        Utf8Pipeline.PPFF(morsel, proj(a), proj(b), pred(c), pred(d))
      case List(Filter(a), Filter(b), Project(c), Filter(d)) =>
        Utf8Pipeline.FFPF(morsel, pred(a), pred(b), proj(c), pred(d))
      case List(Project(a), Filter(b), Project(c), Filter(d)) =>
        Utf8Pipeline.PFPF(morsel, proj(a), pred(b), proj(c), pred(d))
      case List(Filter(a), Project(b), Project(c), Filter(d)) =>
        Utf8Pipeline.FPPF(morsel, pred(a), proj(b), proj(c), pred(d))
      case List(Project(a), Project(b), Project(c), Filter(d)) =>
        Utf8Pipeline.PPPF(morsel, proj(a), proj(b), proj(c), pred(d))
      case List(Filter(a), Filter(b), Filter(c), Project(d)) =>
        Utf8Pipeline.FFFP(morsel, pred(a), pred(b), pred(c), proj(d))
      case List(Project(a), Filter(b), Filter(c), Project(d)) =>
        Utf8Pipeline.PFFP(morsel, proj(a), pred(b), pred(c), proj(d))
      case List(Filter(a), Project(b), Filter(c), Project(d)) =>
        Utf8Pipeline.FPFP(morsel, pred(a), proj(b), pred(c), proj(d))
      case List(Project(a), Project(b), Filter(c), Project(d)) =>
        Utf8Pipeline.PPFP(morsel, proj(a), proj(b), pred(c), proj(d))
      case List(Filter(a), Filter(b), Project(c), Project(d)) =>
        Utf8Pipeline.FFPP(morsel, pred(a), pred(b), proj(c), proj(d))
      case List(Project(a), Filter(b), Project(c), Project(d)) =>
        Utf8Pipeline.PFPP(morsel, proj(a), pred(b), proj(c), proj(d))
      case List(Filter(a), Project(b), Project(c), Project(d)) =>
        Utf8Pipeline.FPPP(morsel, pred(a), proj(b), proj(c), proj(d))
      case List(Project(a), Project(b), Project(c), Project(d)) =>
        Utf8Pipeline.PPPP(morsel, proj(a), proj(b), proj(c), proj(d))
      case _ =>
        throw new IllegalStateException(s"unsupported strings pipeline of length ${steps.length}")
