package com.ewoodbury.scarlet.execution

import scala.collection.mutable

import com.ewoodbury.scarlet.core.{Plan, ScarletConf, StageId}

/**
 * Stage graph for count-sum. Same bypass rule as `countByKey`: an upstream hash at the default
 * width runs the local fold, and anything else shuffles with [[WideOpKind.GroupByKey]] so the
 * writer hashes by key. Int, long, string, and int-long keys each keep their own local fold.
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
    attachWide(
      ctx,
      plan.source,
      plan,
      CountSumByKeyLocalOp().asInstanceOf[Operation[Any, Any]],
      meta => CountSumByKeyWideOp(meta),
      builderMap,
      dependencies,
    )

  def attachLong(
      ctx: StageBuilder.BuildContext,
      plan: Plan.CountSumByLongKeyOp,
      builderMap: mutable.Map[StageId, StageBuilder.StageDraft],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
  ): (StageId, Option[Plan[_]]) =
    attachWide(
      ctx,
      plan.source,
      plan,
      CountSumByLongKeyLocalOp().asInstanceOf[Operation[Any, Any]],
      meta => CountSumByLongKeyWideOp(meta),
      builderMap,
      dependencies,
    )

  def attachString(
      ctx: StageBuilder.BuildContext,
      plan: Plan.CountSumByStringKeyOp,
      builderMap: mutable.Map[StageId, StageBuilder.StageDraft],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
  ): (StageId, Option[Plan[_]]) =
    attachWide(
      ctx,
      plan.source,
      plan,
      CountSumByStringKeyLocalOp().asInstanceOf[Operation[Any, Any]],
      meta => CountSumByStringKeyWideOp(meta),
      builderMap,
      dependencies,
    )

  def attachIntLong(
      ctx: StageBuilder.BuildContext,
      plan: Plan.CountSumByIntLongKeyOp,
      builderMap: mutable.Map[StageId, StageBuilder.StageDraft],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
  ): (StageId, Option[Plan[_]]) =
    attachWide(
      ctx,
      plan.source,
      plan,
      CountSumByIntLongKeyLocalOp().asInstanceOf[Operation[Any, Any]],
      meta => CountSumByIntLongKeyWideOp(meta),
      builderMap,
      dependencies,
    )

  private def attachWide(
      ctx: StageBuilder.BuildContext,
      source: Plan[_],
      plan: Plan[_],
      local: Operation[Any, Any],
      wide: SimpleWideOpMeta => WideOp,
      builderMap: mutable.Map[StageId, StageBuilder.StageDraft],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
  ): (StageId, Option[Plan[_]]) =
    val (sourceStageId, _) =
      StageBuilder.buildStagesFromPlan(ctx, source, builderMap, dependencies)
    val src = builderMap(sourceStageId)
    val defaultN = ScarletConf.get.defaultShufflePartitions
    if (Operation.canBypassShuffle(plan, src.outputPartitioning, ScarletConf.get)) then
      val resultId = StageBuilder.appendOperation(
        ctx,
        sourceStageId,
        local,
        builderMap,
        dependencies,
      )
      (resultId, Some(plan))
    else
      val shuffleId = StageBuilder.createShuffleStageUnified(
        ctx,
        Seq(sourceStageId),
        wide(
          SimpleWideOpMeta(
            kind = WideOpKind.GroupByKey,
            numPartitions = defaultN,
          ),
        ),
        builderMap,
        dependencies,
      )
      (shuffleId, Some(plan))
