package com.ewoodbury.scarlet.execution

import scala.collection.mutable

import com.ewoodbury.scarlet.core.{Plan, ScarletConf, StageId}

/**
 * Row shuffles for the typed sorts. The caller supplies the key and the ordering. A null component
 * is replaced only in the order key. The original row stays null.
 */
private[execution] object SortStages:

  /** Effective string key. A null key orders as `""`. */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def stringKey(pair: (String, Int)): String =
    pair._1 match
      case null => ""
      case key => key

  /**
   * Order key for `(Long, Long)` value sort: value descending, then key ascending. A null orders
   * as zero. The pair itself is not rewritten.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Product",
    ),
  )
  def longValueKey(pair: (Long, Long)): (Long, Long) =
    (longOrZero(pair.productElement(1)), longOrZero(pair.productElement(0)))

  /**
   * Order key for `(String, Int)` value sort: value descending, then key ascending. A null value
   * orders as zero. A null key orders as `""`. The pair itself is not rewritten.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Product",
    ),
  )
  def stringValueKey(pair: (String, Int)): (Int, String) =
    (intOrZero(pair.productElement(1)), stringOrEmpty(pair.productElement(0)))

  /**
   * Range-partitions and locally sorts. The typed sorts share this shuffle. The caller supplies
   * the key function and the ordering.
   */
  def shuffle[A, B](
      ctx: StageBuilder.BuildContext,
      source: Plan[A],
      keyFunc: A => B,
      ordering: Ordering[B],
      plan: Plan[_],
      builderMap: mutable.Map[StageId, StageBuilder.StageDraft],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
  ): (StageId, Option[Plan[_]]) =
    val (sourceStageId, _) =
      StageBuilder.buildStagesFromPlan(ctx, source, builderMap, dependencies)
    val numPartitions = ScarletConf.get.defaultShufflePartitions
    val shuffleId = StageBuilder.createShuffleStageUnified(
      ctx,
      Seq(sourceStageId),
      SortByWideOp(
        SortWideOpMeta(
          numPartitions = numPartitions,
          keyFunc = keyFunc,
          ordering = ordering,
        ),
      ),
      builderMap,
      dependencies,
    )
    (shuffleId, Some(plan))

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Product",
    ),
  )
  private def longOrZero(cell: Any): Long = cell match
    case null => 0L
    case value: Long => value
    case _ => 0L

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def intOrZero(cell: Any): Int = cell match
    case null => 0
    case value: Int => value
    case _ => 0

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def stringOrEmpty(cell: Any): String = cell match
    case null => ""
    case value: String => value
    case _ => ""
