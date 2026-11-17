package com.ewoodbury.scarlet.execution

import scala.collection.mutable

import com.ewoodbury.scarlet.core.{Plan, ScarletConf, StageId}

/**
 * Stage graph for [[Plan.CountSumByKeyOp]]. Same bypass rule as `countByKey`: an upstream hash at
 * the default width runs the local fold, and anything else shuffles with [[WideOpKind.GroupByKey]]
 * so the writer hashes by key.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.MutableDataStructures",
  ),
)
private[execution] object CountSumStages:

  def attach(
      ctx: StageBuilder.BuildContext,
      plan: Plan.CountSumByKeyOp,
      builderMap: mutable.Map[StageId, StageBuilder.StageDraft],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
  ): (StageId, Option[Plan[_]]) =
    val (sourceStageId, _) =
      StageBuilder.buildStagesFromPlan(ctx, plan.source, builderMap, dependencies)
    val src = builderMap(sourceStageId)
    val defaultN = ScarletConf.get.defaultShufflePartitions
    if (Operation.canBypassShuffle(plan, src.outputPartitioning, ScarletConf.get)) then
      val resultId = StageBuilder.appendOperation(
        ctx,
        sourceStageId,
        CountSumByKeyLocalOp().asInstanceOf[Operation[Any, Any]],
        builderMap,
        dependencies,
      )
      (resultId, Some(plan))
    else
      val shuffleId = StageBuilder.createShuffleStageUnified(
        ctx,
        Seq(sourceStageId),
        CountSumByKeyWideOp(
          SimpleWideOpMeta(
            kind = WideOpKind.GroupByKey,
            numPartitions = defaultN,
          ),
        ),
        builderMap,
        dependencies,
      )
      (shuffleId, Some(plan))
