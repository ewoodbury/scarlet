package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.GroupByKey
import com.ewoodbury.scarlet.core.Partition

/**
 * Represents a stage: a sequence of narrow transformations that can be executed together. Each
 * stage operates on partitions independently without requiring shuffles.
 */
sealed trait Stage[A, B] {
  def execute(partition: Partition[A]): Partition[B]
}

object Stage {

  /** A stage that applies a single transformation */
  case class SingleOpStage[A, B](operation: Partition[A] => Partition[B]) extends Stage[A, B] {
    def execute(partition: Partition[A]): Partition[B] = operation(partition)
  }

  /** A stage that chains multiple transformations */
  case class ChainedStage[A, B, C](
      first: Stage[A, B],
      second: Stage[B, C],
  ) extends Stage[A, C] {
    def execute(partition: Partition[A]): Partition[C] =
      second.execute(first.execute(partition))
  }

  // Factory methods for common operations
  def map[A, B](f: A => B): Stage[A, B] = {
    SingleOpStage { p =>
      val it = p.data.iterator.map(f)
      Partition(IterUtil.iterableOf(it))
    }
  }

  def filter[A](predicate: A => Boolean): Stage[A, A] = {
    SingleOpStage { p =>
      val it = p.data.iterator.filter(predicate)
      Partition(IterUtil.iterableOf(it))
    }
  }

  def flatMap[A, B](f: A => IterableOnce[B]): Stage[A, B] = {
    SingleOpStage { p =>
      val it = p.data.iterator.flatMap(a => f(a).iterator)
      Partition(IterUtil.iterableOf(it))
    }
  }

  /**
   * Partition-level transform using iterators to avoid unnecessary materialization.
   */
  def mapPartitions[A, B](f: Iterator[A] => Iterator[B]): Stage[A, B] = {
    SingleOpStage { p =>
      val it = f(p.data.iterator)
      Partition(IterUtil.iterableOf(it))
    }
  }

  /**
   * Local distinct of one partition. Global distinct hash-shuffles by the element first, then runs
   * this on each shuffle partition. The view stays stable across traversals of the same partition.
   */
  def distinct[A]: Stage[A, A] = {
    SingleOpStage { p =>
      val it = p.data.iterator.distinct
      Partition(IterUtil.iterableOf(it))
    }
  }

  // Key-value operations
  def keys[K, V]: Stage[(K, V), K] = {
    SingleOpStage { p =>
      val it = p.data.iterator.map(_._1)
      Partition(IterUtil.iterableOf(it))
    }
  }

  def values[K, V]: Stage[(K, V), V] = {
    SingleOpStage { p =>
      val it = p.data.iterator.map(_._2)
      Partition(IterUtil.iterableOf(it))
    }
  }

  def mapValues[K, V, B](f: V => B): Stage[(K, V), (K, B)] = {
    SingleOpStage { p =>
      val it = p.data.iterator.map { case (k, v) => (k, f(v)) }
      Partition(IterUtil.iterableOf(it))
    }
  }

  /**
   * Maps an int key. `productElement` reads a null key without unboxing it. The function is not
   * called for that row, and the value is left as stored.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  def mapIntKey(f: Int => Int): Stage[(Any, Any), (Any, Any)] = {
    SingleOpStage { partition =>
      val mapped = partition.data.iterator.map { row =>
        val product = row.asInstanceOf[Product]
        product.productElement(0) match {
          case null => row
          case present =>
            Tuple2(Int.box(f(present.asInstanceOf[Int])), product.productElement(1))
              .asInstanceOf[(Any, Any)]
        }
      }
      Partition(IterUtil.iterableOf(mapped))
    }
  }

  def filterKeys[K, V](predicate: K => Boolean): Stage[(K, V), (K, V)] = {
    SingleOpStage { p =>
      val it = p.data.iterator.filter { case (k, _) => predicate(k) }
      Partition(IterUtil.iterableOf(it))
    }
  }

  def filterValues[K, V](predicate: V => Boolean): Stage[(K, V), (K, V)] = {
    SingleOpStage { p =>
      val it = p.data.iterator.filter { case (_, v) => predicate(v) }
      Partition(IterUtil.iterableOf(it))
    }
  }

  def flatMapValues[K, V, B](f: V => IterableOnce[B]): Stage[(K, V), (K, B)] = {
    SingleOpStage { p =>
      val it = p.data.iterator.flatMap { case (k, v) => f(v).iterator.map(b => (k, b)) }
      Partition(IterUtil.iterableOf(it))
    }
  }

  /**
   * Local keyed groupBy on a single partition. Used for shuffle-bypass groupByKey and as the
   * per-partition operator of a groupByKey shuffle stage (input is already key-partitioned). A
   * null key is dropped before grouping. `groupMap` is a single pass; value order inside each
   * group follows input order.
   */
  def groupByKeyLocal[K, V]: Stage[(K, V), (K, Iterable[V])] = {
    SingleOpStage { p =>
      val grouped = NullKeys.drop(p.data).groupMap(_._1)(_._2).toSeq
      GroupByKey.noteValueLists(grouped.size)
      Partition(grouped)
    }
  }

  /**
   * Local keyed count on a single partition. Used for shuffle-bypass countByKey and as the
   * per-partition operator of a countByKey shuffle stage (input is already key-partitioned). A
   * null key is dropped. The value is not read, so a null value still counts. The count is an
   * `Int` and wraps on overflow.
   */
  def countByKeyLocal[K, V]: Stage[(K, V), (K, Int)] = {
    SingleOpStage { p =>
      Partition(
        NullKeys
          .drop(p.data)
          .groupMapReduce(_._1)((_: (K, V)) => 1)((left, right) => left + right)
          .toSeq,
      )
    }
  }

  /**
   * Local count and sum on a single partition. A null key is dropped. A null value is counted and
   * not added. The count is an `Int` and wraps. Used for shuffle-bypass and as the per-partition
   * operator of a count-sum shuffle stage.
   */
  def countSumByKeyLocal: Stage[(Any, Any), (Int, Int, Long)] = {
    SingleOpStage { p =>
      Partition(CountSumRows.of(p.data))
    }
  }

  /**
   * Local keyed reduce on a single partition. Used for shuffle-bypass reduceByKey and as the
   * per-partition operator of a reduceByKey shuffle stage (input is already key-partitioned). A
   * null key is dropped before reducing. `groupMapReduce` is a single pass; a key with one value
   * is returned unchanged.
   */
  def reduceByKeyLocal[K, V](op: (V, V) => V): Stage[(K, V), (K, V)] = {
    SingleOpStage { p =>
      Partition(NullKeys.drop(p.data).groupMapReduce(_._1)(_._2)(op).toSeq)
    }
  }

  /**
   * Local sort of a range-partitioned `(sortKey, element)` partition, emitting elements in sort
   * key order. Used by the sortBy shuffle stage; concatenating these partitions in index order
   * yields a globally ordered result.
   */
  def sortLocal[S, A](ordering: Ordering[S]): Stage[(S, A), A] = {
    SingleOpStage { p =>
      Partition(p.data.toSeq.sortBy(_._1)(ordering).map(_._2))
    }
  }

  /**
   * Identity operation that returns the input partition unchanged. Used for bypassed operations
   * that don't need to transform the data.
   */
  def identity[A]: Stage[A, A] = {
    SingleOpStage { p => p }
  }
}
