package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.PairStep
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}

/**
 * Structural peel label for the pair steps a grouped sum walks.
 *
 * This is not an action shape, not a [[com.ewoodbury.scarlet.core.Plan]] node, and not a
 * [[PhysicalOp]]. It names the filters and pair maps [[ColumnarPlanner.peelPairOps]] walks above a
 * grouped int-pair source, in source order. A typed int-pair source is `GroupedSum(Scan)`. Those
 * steps wrap the scan as `FilterRow`, `FilterKey`, `FilterValue`, or `MapPair`. The label does not
 * mean [[ColumnarPlanner.tryGroupedSum]] runs. A map that changes `(Int, Int)` to another pair
 * type still peels as `GroupedSum(MapPair(Scan))`, and the fused action returns `None`.
 * [[ColumnarPlanner.lower]] refuses every pair map, so [[ColumnarPlanner.partitioning]] reports no
 * layout for it.
 */
private[execution] object GroupedSumPeel:

  def of(plan: Plan[_]): Option[String] =
    if (!ScarletConf.get.columnarExecution) then None
    else
      val (base, steps) = ColumnarPlanner.peelPairOps(plan)
      base match
        case source: Plan.Source[_] if ColumnarPlanner.groupedPairSource(source) =>
          Some(render(steps))
        case _ => None

  private def render(steps: List[PairStep]): String =
    val body = steps.foldLeft("Scan")((child, step) => s"${label(step)}($child)")
    s"GroupedSum($body)"

  private def label(step: PairStep): String = step match
    case PairStep.KeepKey(_) => "FilterKey"
    case PairStep.KeepValue(_) => "FilterValue"
    case PairStep.KeepRow(_) => "FilterRow"
    case PairStep.MapPair(_) => "MapPair"
