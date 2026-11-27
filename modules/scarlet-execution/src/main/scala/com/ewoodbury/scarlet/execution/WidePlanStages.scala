package com.ewoodbury.scarlet.execution

import scala.collection.mutable

import com.ewoodbury.scarlet.core.{Plan, StageId}

/**
 * Stage graph for a wide-row filter or project. Both are narrow: they keep the upstream
 * partitioning and run inside the source stage.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.MutableDataStructures",
  ),
)
private[execution] object WidePlanStages:

  def attachFilter(
      ctx: StageBuilder.BuildContext,
      plan: Plan.WideFilterOp,
      builderMap: mutable.Map[StageId, StageBuilder.StageDraft],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
  ): (StageId, Option[Plan[_]]) =
    attach(
      ctx,
      plan.source,
      WideFilterOp(plan.ordinal, plan.compare),
      plan,
      builderMap,
      dependencies,
    )

  def attachProject(
      ctx: StageBuilder.BuildContext,
      plan: Plan.WideProjectOp,
      builderMap: mutable.Map[StageId, StageBuilder.StageDraft],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
  ): (StageId, Option[Plan[_]]) =
    attach(ctx, plan.source, WideProjectOp(plan.spec), plan, builderMap, dependencies)

  private def attach(
      ctx: StageBuilder.BuildContext,
      source: Plan[com.ewoodbury.scarlet.core.WideRow],
      op: Operation[com.ewoodbury.scarlet.core.WideRow, com.ewoodbury.scarlet.core.WideRow],
      plan: Plan[_],
      builderMap: mutable.Map[StageId, StageBuilder.StageDraft],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
  ): (StageId, Option[Plan[_]]) =
    val (sourceStageId, _) =
      StageBuilder.buildStagesFromPlan(ctx, source, builderMap, dependencies)
    val resultId = StageBuilder.appendOperation(
      ctx,
      sourceStageId,
      op.asInstanceOf[Operation[Any, Any]],
      builderMap,
      dependencies,
    )
    (resultId, Some(plan))
