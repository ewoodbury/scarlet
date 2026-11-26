package com.ewoodbury.scarlet.execution

import scala.collection.mutable

import com.ewoodbury.scarlet.core.{Plan, ScarletConf, StageId}

/**
 * Stage graph for [[Plan.DistinctOp]]. The row path hash-shuffles by the element, then keeps one
 * copy in each shuffle partition. There is no bypass: element hash is not pair-key hash, so an
 * upstream [[PartitioningInfo.hash]] does not mean duplicates are already co-located.
 */
@SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
private[execution] object DistinctStages:

  def attach(
      ctx: StageBuilder.BuildContext,
      plan: Plan.DistinctOp[_],
      builderMap: mutable.Map[StageId, StageBuilder.StageDraft],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
  ): (StageId, Option[Plan[_]]) =
    val (sourceStageId, _) =
      StageBuilder.buildStagesFromPlan(ctx, plan.source, builderMap, dependencies)
    val numPartitions = ScarletConf.get.defaultShufflePartitions
    val shuffleId = StageBuilder.createShuffleStageUnified(
      ctx,
      Seq(sourceStageId),
      DistinctWideOp(
        SimpleWideOpMeta(
          kind = WideOpKind.Distinct,
          numPartitions = numPartitions,
        ),
      ),
      builderMap,
      dependencies,
    )
    (shuffleId, Some(plan))
