package com.ewoodbury.scarlet.execution

/**
 * The 28 filter/project shapes of length two, three, and four.
 *
 * [[Shape.of]] is the only match on that order. Each case holds the erased step functions, left to
 * right. A caller matches the shape and passes those functions to its own kernel. The typed cast
 * stays at that call. Five steps are not a shape, and neither is any other length.
 *
 * `F` is a filter and `P` is a project. Case order is the existing kernel order. The leftmost step
 * is the low bit, so length two is `FF`, `PF`, `FP`, `PP`.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Throw",
  ),
)
private[execution] enum Shape:

  // Payloads are the erased functions already stored on ElementStep. No cast happens here.
  case FF(a: Any, b: Any)
  case PF(a: Any, b: Any)
  case FP(a: Any, b: Any)
  case PP(a: Any, b: Any)
  case FFF(a: Any, b: Any, c: Any)
  case PFF(a: Any, b: Any, c: Any)
  case FPF(a: Any, b: Any, c: Any)
  case PPF(a: Any, b: Any, c: Any)
  case FFP(a: Any, b: Any, c: Any)
  case PFP(a: Any, b: Any, c: Any)
  case FPP(a: Any, b: Any, c: Any)
  case PPP(a: Any, b: Any, c: Any)
  case FFFF(a: Any, b: Any, c: Any, d: Any)
  case PFFF(a: Any, b: Any, c: Any, d: Any)
  case FPFF(a: Any, b: Any, c: Any, d: Any)
  case PPFF(a: Any, b: Any, c: Any, d: Any)
  case FFPF(a: Any, b: Any, c: Any, d: Any)
  case PFPF(a: Any, b: Any, c: Any, d: Any)
  case FPPF(a: Any, b: Any, c: Any, d: Any)
  case PPPF(a: Any, b: Any, c: Any, d: Any)
  case FFFP(a: Any, b: Any, c: Any, d: Any)
  case PFFP(a: Any, b: Any, c: Any, d: Any)
  case FPFP(a: Any, b: Any, c: Any, d: Any)
  case PPFP(a: Any, b: Any, c: Any, d: Any)
  case FFPP(a: Any, b: Any, c: Any, d: Any)
  case PFPP(a: Any, b: Any, c: Any, d: Any)
  case FPPP(a: Any, b: Any, c: Any, d: Any)
  case PPPP(a: Any, b: Any, c: Any, d: Any)

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Throw",
  ),
)
object Shape:

  /** `None` when the chain is not one of the 28 shapes. */
  def of(steps: List[ColumnarPlanner.ElementStep]): Option[Shape] =
    import ColumnarPlanner.ElementStep.*

    steps match
      case List(Filter(a), Filter(b)) => Some(FF(a, b))
      case List(Project(a), Filter(b)) => Some(PF(a, b))
      case List(Filter(a), Project(b)) => Some(FP(a, b))
      case List(Project(a), Project(b)) => Some(PP(a, b))
      case List(Filter(a), Filter(b), Filter(c)) => Some(FFF(a, b, c))
      case List(Project(a), Filter(b), Filter(c)) => Some(PFF(a, b, c))
      case List(Filter(a), Project(b), Filter(c)) => Some(FPF(a, b, c))
      case List(Project(a), Project(b), Filter(c)) => Some(PPF(a, b, c))
      case List(Filter(a), Filter(b), Project(c)) => Some(FFP(a, b, c))
      case List(Project(a), Filter(b), Project(c)) => Some(PFP(a, b, c))
      case List(Filter(a), Project(b), Project(c)) => Some(FPP(a, b, c))
      case List(Project(a), Project(b), Project(c)) => Some(PPP(a, b, c))
      case List(Filter(a), Filter(b), Filter(c), Filter(d)) => Some(FFFF(a, b, c, d))
      case List(Project(a), Filter(b), Filter(c), Filter(d)) => Some(PFFF(a, b, c, d))
      case List(Filter(a), Project(b), Filter(c), Filter(d)) => Some(FPFF(a, b, c, d))
      case List(Project(a), Project(b), Filter(c), Filter(d)) => Some(PPFF(a, b, c, d))
      case List(Filter(a), Filter(b), Project(c), Filter(d)) => Some(FFPF(a, b, c, d))
      case List(Project(a), Filter(b), Project(c), Filter(d)) => Some(PFPF(a, b, c, d))
      case List(Filter(a), Project(b), Project(c), Filter(d)) => Some(FPPF(a, b, c, d))
      case List(Project(a), Project(b), Project(c), Filter(d)) => Some(PPPF(a, b, c, d))
      case List(Filter(a), Filter(b), Filter(c), Project(d)) => Some(FFFP(a, b, c, d))
      case List(Project(a), Filter(b), Filter(c), Project(d)) => Some(PFFP(a, b, c, d))
      case List(Filter(a), Project(b), Filter(c), Project(d)) => Some(FPFP(a, b, c, d))
      case List(Project(a), Project(b), Filter(c), Project(d)) => Some(PPFP(a, b, c, d))
      case List(Filter(a), Filter(b), Project(c), Project(d)) => Some(FFPP(a, b, c, d))
      case List(Project(a), Filter(b), Project(c), Project(d)) => Some(PFPP(a, b, c, d))
      case List(Filter(a), Project(b), Project(c), Project(d)) => Some(FPPP(a, b, c, d))
      case List(Project(a), Project(b), Project(c), Project(d)) => Some(PPPP(a, b, c, d))
      case _ => None

  /**
   * The shared refusal for a chain outside the 28 shapes. `kind` is the column family in the
   * existing message (`ints`, `floats`, or `strings`). A site with its own fallback does not call
   * this.
   */
  def unsupported(kind: String, length: Int): Nothing =
    throw new IllegalStateException(s"unsupported $kind pipeline of length $length")
