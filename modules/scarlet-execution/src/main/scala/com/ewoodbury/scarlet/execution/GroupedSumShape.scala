package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.PairStep
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}

/**
 * Shape string for the fused grouped-sum action.
 *
 * This is not a [[com.ewoodbury.scarlet.core.Plan]] node and not a [[PhysicalOp]]. A typed
 * int-pair source is `GroupedSum(Scan)`. Pair filters and a pair map wrap that scan in source
 * order with `FilterRow`, `FilterKey`, `FilterValue`, or `MapPair`. [[ColumnarPlanner.lower]]
 * refuses the map, so [[ColumnarPlanner.partitioning]] does not report a layout for it.
 */
private[execution] object GroupedSumShape:

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
