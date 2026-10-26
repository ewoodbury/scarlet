package com.ewoodbury.scarlet.execution

import com.typesafe.scalalogging.{Logger, StrictLogging}

import com.ewoodbury.scarlet.core.Partition
import com.ewoodbury.scarlet.runtime.api.RunnableTask

/**
 * A Task represents a unit of computation that can be run on an executor. It operates on a single
 * input partition to produce a single output partition.
 */
sealed trait Task[A, B] extends RunnableTask[A, B]:
  def partition: Partition[A]
  def run(): Partition[B]

object Task extends StrictLogging:
  private val taskLogger: Logger = logger

  /** Creates a StageTask from a partition and stage. */
  def createStageTask[A, B](partition: Partition[A], stage: Stage[A, B]): StageTask[A, B] =
    StageTask(partition, stage)

  /** Creates a shuffle hash join task. */
  def createShuffleHashJoinTask[K, L, R](
      leftData: Seq[(K, L)],
      rightData: Seq[(K, R)],
  ): ShuffleHashJoinTask[K, L, R] = ShuffleHashJoinTask(leftData, rightData)

  /** Creates a shuffle-hash left outer join task. */
  def createShuffleHashLeftOuterJoinTask[K, L, R](
      leftData: Seq[(K, L)],
      rightData: Seq[(K, R)],
  ): ShuffleHashLeftOuterJoinTask[K, L, R] = ShuffleHashLeftOuterJoinTask(leftData, rightData)

  /** Creates a broadcast hash join task. */
  def createBroadcastHashJoinTask[K, L, R](
      localData: Seq[(K, L)],
      broadcastMap: Map[K, Seq[R]],
      isRightLocal: Boolean = false,
  ): BroadcastHashJoinTask[K, L, R] = BroadcastHashJoinTask(localData, broadcastMap, isRightLocal)

  /** Creates a sort merge join task with proper ordering. */
  def createSortMergeJoinTask[K: Ordering, L, R](
      leftData: Seq[(K, L)],
      rightData: Seq[(K, R)],
  ): SortMergeJoinTask[K, L, R] = SortMergeJoinTask(leftData, rightData)

  /** Creates a per-partition cogroup task over two co-partitioned inputs. */
  def createCogroupTask[K, L, R](
      leftData: Seq[(K, L)],
      rightData: Seq[(K, R)],
  ): CogroupTask[K, L, R] = CogroupTask(leftData, rightData)

  /** A task that executes a complete stage (chain of narrow transformations) on a partition. */
  case class StageTask[A, B](
      partition: Partition[A],
      stage: Stage[A, B],
  ) extends Task[A, B]:
    override def run(): Partition[B] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] StageTask on partition")
      stage.execute(partition)
    }

  /**
   * A per-partition cogroup task over two co-partitioned inputs. Includes keys present on either
   * side, with an empty sequence for a missing side. A null key is dropped and does not form a
   * group.
   */
  final case class CogroupTask[K, L, R](
      leftData: Seq[(K, L)],
      rightData: Seq[(K, R)],
  ) extends RunnableTask[Any, (K, (Iterable[L], Iterable[R]))]:
    override def run(): Partition[(K, (Iterable[L], Iterable[R]))] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] CogroupTask on partition")
      val leftGrouped = NullKeys.drop(leftData).groupMap(_._1)(_._2)
      val rightGrouped = NullKeys.drop(rightData).groupMap(_._1)(_._2)
      val result = (leftGrouped.keySet union rightGrouped.keySet).iterator.map { key =>
        (
          key,
          (
            leftGrouped.getOrElse(key, Seq.empty[L]),
            rightGrouped.getOrElse(key, Seq.empty[R]),
          ),
        )
      }.toSeq
      Partition(result)
    }

  /**
   * A per-partition shuffle-hash inner join task that joins two co-partitioned inputs. A null key
   * is dropped on both sides and does not match.
   */
  final case class ShuffleHashJoinTask[K, L, R](
      leftData: Seq[(K, L)],
      rightData: Seq[(K, R)],
  ) extends RunnableTask[Any, (K, (L, R))]:
    override def run(): Partition[(K, (L, R))] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] ShuffleHashJoinTask on partition")

      // Build hash maps for both sides to avoid unsafe casting
      val leftGrouped = NullKeys.drop(leftData).groupBy(_._1).view.mapValues(_.map(_._2)).toMap
      val rightGrouped =
        NullKeys.drop(rightData).groupBy(_._1).view.mapValues(_.map(_._2).toSeq).toMap

      // Find all common keys and perform cross product
      val commonKeys = leftGrouped.keySet.intersect(rightGrouped.keySet)
      val result = for {
        key <- commonKeys.toSeq
        leftValue <- leftGrouped(key)
        rightValue <- rightGrouped(key)
      } yield (key, (leftValue, rightValue))

      Partition(result)
    }

  /**
   * A per-partition shuffle-hash left outer join. Matches emit the cartesian product. Each left
   * row whose key is absent on the right is emitted once with `None`. A null key never matches.
   */
  final case class ShuffleHashLeftOuterJoinTask[K, L, R](
      leftData: Seq[(K, L)],
      rightData: Seq[(K, R)],
  ) extends RunnableTask[Any, (K, (L, Option[R]))]:
    @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Equals"))
    override def run(): Partition[(K, (L, Option[R]))] = {
      taskLogger.debug(
        s"[${Thread.currentThread().getName}] ShuffleHashLeftOuterJoinTask on partition",
      )
      val rightGrouped =
        NullKeys.drop(rightData).groupBy(_._1).view.mapValues(_.map(_._2).toSeq).toMap

      def rightValues(key: K): Seq[R] =
        if (key == null) Seq.empty[R]
        else rightGrouped.getOrElse(key, Seq.empty[R])

      val result = leftData.flatMap { case (key, leftValue) =>
        val matches = rightValues(key)
        if (matches.isEmpty) Seq((key, (leftValue, None)))
        else matches.map(rightValue => (key, (leftValue, Some(rightValue))))
      }
      Partition(result)
    }

  /**
   * A per-partition sort-merge inner join task that joins two sorted, co-partitioned inputs. A
   * null key is dropped on both sides before ordering, so it is not compared and does not match.
   */
  final case class SortMergeJoinTask[K, L, R](
      leftData: Seq[(K, L)],
      rightData: Seq[(K, R)],
  )(using keyOrdering: Ordering[K])
      extends RunnableTask[Any, (K, (L, R))]:
    override def run(): Partition[(K, (L, R))] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] SortMergeJoinTask on partition")

      // Sort both sides by key if they're not already sorted. Null keys are already gone.
      val sortedLeft = NullKeys.drop(leftData).toSeq.sortBy(_._1)
      val sortedRight = NullKeys.drop(rightData).toSeq.sortBy(_._1)

      // Group by key and perform cross product for matching keys
      val leftByKey = sortedLeft.groupBy(_._1).view.mapValues(_.map(_._2)).toMap
      val rightByKey = sortedRight.groupBy(_._1).view.mapValues(_.map(_._2)).toMap

      val result = for {
        (key, leftValues) <- leftByKey.toSeq
        rightValues <- rightByKey.get(key).toSeq
        leftValue <- leftValues
        rightValue <- rightValues
      } yield (key, (leftValue, rightValue))

      Partition(result)
    }

  /**
   * A broadcast-hash inner join task that joins a local partition with a broadcast dataset.
   *
   * Named erasure boundary: the task operates on erased (Any, Any) records because join sides
   * travel through the shuffle service; when the broadcast side is the join's left input the
   * values are re-typed at the tuple level. Safe because the caller broadcasts and reads the same
   * side consistently for a given join stage. A null key is dropped from the local rows. The
   * broadcast map is built without null keys, so a null key does not match.
   */
  final case class BroadcastHashJoinTask[K, L, R](
      localData: Seq[(K, L)],
      broadcastMap: Map[K, Seq[R]],
      isRightLocal: Boolean,
  ) extends RunnableTask[Any, (K, (L, R))]:
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    override def run(): Partition[(K, (L, R))] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] BroadcastHashJoinTask on partition")
      val result = NullKeys.drop(localData).iterator.flatMap { case (k, localValue) =>
        broadcastMap.getOrElse(k, Seq.empty[R]).iterator.map { broadcastValue =>
          if (isRightLocal) {
            // Local data is right side (R), broadcast is left side (L)
            // This is a type system limitation - we need the cast here
            (k, (broadcastValue.asInstanceOf[L], localValue.asInstanceOf[R]))
          } else {
            // Local data is left side (L), broadcast is right side (R) - this is the normal case
            (k, (localValue, broadcastValue))
          }
        }
      }
      Partition(IterUtil.iterableOf(result))
    }

end Task
