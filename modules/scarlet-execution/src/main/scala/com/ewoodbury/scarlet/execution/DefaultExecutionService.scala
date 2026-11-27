package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.PairStep
import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Bridges the API to the execution engine. A bare source returns its partitions. Plans that
 * [[ColumnarPlanner]] accepts run on the columnar kernels. A `mapPartitions` tail runs that child
 * first, then the row function. A `flatMap` keeps a decoded child only when those partitions equal
 * the row executor's, in order, and then runs the flat-map function. `flatMapValues` uses that
 * same decode and runs the value function. It does not grow a second decoder. An untyped `sortBy`
 * sorts that decoded child with the row scheduler. A typed sort stays on its kernel. A `cogroup`
 * decodes each child on its own. One disagreement runs the original plan on rows. A `map` or
 * `mapValues` that [[ColumnarPlanner.tryRun]] accepts stays on that kernel. A refused one keeps a
 * decoded child and runs the row function. A same-type map never takes that boundary. A mismatch
 * runs the original plan on rows. A non-source child of `mapPartitions` is kept only when its
 * decoded partitions equal the row executor's, in order. A lowered filter that drops a null, or a
 * decoder that rejects a null pair, runs that child on rows. A child the planner refuses publishes
 * nothing. A join with one lowered side decodes that side and runs the join on the row executor
 * when those rows match. `take` asks [[ColumnarPlanner.tryTake]] and `count` asks
 * [[ColumnarPlanner.tryCount]] before either splits the original plan. A miss on `count` of a
 * `repartition`, `coalesce`, or `partitionBy` counts that child and does not shuffle. A `map`
 * still runs once per row. Neither wraps that plan in `MapPartitionsOp` first. Every other refused
 * plan is compiled into a stage graph and executed through the DAGScheduler.
 */
class DefaultExecutionService extends ExecutionService {

  def execute[A](plan: Plan[A]): Seq[A] =
    executePartitions(plan).flatMap(_.data)

  def executePartitions[A](plan: Plan[A]): Seq[Partition[A]] = plan match {
    case s: Plan.Source[A] =>
      // A bare source needs no tasks; its partitions are the result
      s.partitions

    case Plan.MapPartitionsOp(source, f) =>
      MixedAction.tail(source, f)

    case Plan.FlatMapOp(source, f) =>
      RowBoundary.flatMap(source, f).getOrElse(runRow(plan))

    case Plan.FlatMapValuesOp(source, f) =>
      RowBoundary.flatMapValues(source, f).getOrElse(runRow(plan))

    case Plan.SortByOp(source, keyFunc, ordering) =>
      RowBoundary.sortBy(source, keyFunc, ordering).getOrElse(runRow(plan))

    case Plan.CoGroupOp(left, right) =>
      RowBoundary.cogroup(left, right).getOrElse(runRow(plan))

    case Plan.MapOp(source, f) =>
      ColumnarPlanner
        .tryRun(plan)
        .getOrElse(RowBoundary.map(source, f).getOrElse(runRow(plan)))

    case Plan.MapValuesOp(source, mapFunction) =>
      ColumnarPlanner
        .tryRun(plan)
        .getOrElse(RowBoundary.mapValues(source, mapFunction).getOrElse(runRow(plan)))

    case join: Plan.WideJoinOp =>
      wideJoinPartitions(plan, join)

    case group: Plan.WideGroupOp =>
      wideGroupPartitions(plan, group)

    case _ =>
      ColumnarPlanner.tryRun(plan).orElse(MixedJoin.partitions(plan)).getOrElse(runRow(plan))
  }

  /**
   * Columnar wide join when it lowers. Otherwise the in-memory wide join. `Plan` is invariant, so
   * the row partitions are cast back to the caller's element type. [[Plan.WideJoinOp]] is always
   * `Plan[WideRow]`.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def wideJoinPartitions[A](plan: Plan[A], join: Plan.WideJoinOp): Seq[Partition[A]] =
    ColumnarPlanner.tryRun(plan).getOrElse {
      WideJoinRows.partitions(join).asInstanceOf[Seq[Partition[A]]]
    }

  /**
   * Columnar wide group when it lowers. Otherwise the in-memory wide group. `Plan` is invariant,
   * so the row partitions are cast back to the caller's element type. [[Plan.WideGroupOp]] is
   * always `Plan[WideRow]`.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def wideGroupPartitions[A](plan: Plan[A], group: Plan.WideGroupOp): Seq[Partition[A]] =
    ColumnarPlanner.tryRun(plan).getOrElse {
      WideGroupRows.partitions(group).asInstanceOf[Seq[Partition[A]]]
    }

  def count[A](plan: Plan[A]): Long =
    ColumnarPlanner.tryCount(plan).getOrElse(countAfterMiss(plan))

  /**
   * Count when no columnar count exists. `repartition`, `coalesce`, and `partitionBy` keep every
   * row, including a null key, so the count is the child's count and no shuffle is written. A
   * `map` is not peeled: a refused map runs once per row.
   */
  private def countAfterMiss[A](plan: Plan[A]): Long =
    widthChild(plan) match
      case Some(child) => count(child)
      case None =>
        MixedAction
          .partitions(plan)
          .foldLeft(0L)((total, partition) => total + partition.data.size)

  private def widthChild(plan: Plan[_]): Option[Plan[_]] =
    plan match
      case Plan.RepartitionOp(source, _) => Some(source)
      case Plan.CoalesceOp(source, _) => Some(source)
      case Plan.PartitionByOp(source, _) => Some(source)
      case _ => None

  def sumInt(plan: Plan[Int]): Long =
    ColumnarPlanner.trySumInt(plan).getOrElse {
      rowPartitions(plan).foldLeft(0L) { (total, partition) =>
        partition.data.foldLeft(total)((acc, value) => acc + value.toLong)
      }
    }

  def sumLong(plan: Plan[Long]): Long =
    ColumnarPlanner.trySumLong(plan).getOrElse {
      rowPartitions(plan).foldLeft(0L) { (total, partition) =>
        partition.data.foldLeft(total)((acc, value) => acc + value)
      }
    }

  def sumWideLong(plan: Plan[WideRow], ordinal: Int): Long =
    WideSum.longColumn(plan, ordinal).getOrElse {
      rowPartitions(plan).foldLeft(0L) { (total, partition) =>
        partition.data.foldLeft(total)((acc, row) => addLongCell(acc, row, ordinal))
      }
    }

  def sumWideDecimal(plan: Plan[WideRow], ordinal: Int): Long =
    WideSum.decimalColumn(plan, ordinal).getOrElse {
      rowPartitions(plan).foldLeft(0L) { (total, partition) =>
        partition.data.foldLeft(total)((acc, row) => addDecimalCell(acc, row, ordinal))
      }
    }

  private def addLongCell(total: Long, row: WideRow, ordinal: Int): Long =
    row.cells(ordinal) match
      case Cell.Int64(value) => total + value
      case Cell.Null => total
      case other =>
        throw new IllegalArgumentException(s"column $ordinal is $other, expected a long")

  private def addDecimalCell(total: Long, row: WideRow, ordinal: Int): Long =
    row.cells(ordinal) match
      case Cell.Decimal(_, _, value) =>
        val sum =
          try Math.addExact(total, value)
          catch
            case _: ArithmeticException =>
              throw new IllegalArgumentException(
                s"decimal value does not fit precision ${DecimalRules.MaxPrecision}",
              )
        if (!DecimalRules.fits(sum, DecimalRules.MaxPrecision)) then
          throw new IllegalArgumentException(
            s"decimal value does not fit precision ${DecimalRules.MaxPrecision}",
          )
        sum
      case Cell.Null => total
      case other =>
        throw new IllegalArgumentException(s"column $ordinal is $other, expected a decimal")

  def minWideInt(plan: Plan[WideRow], ordinal: Int): Int =
    WideExtremum.minInt(plan, ordinal).getOrElse(foldInt(plan, ordinal, least = true))

  def maxWideInt(plan: Plan[WideRow], ordinal: Int): Int =
    WideExtremum.maxInt(plan, ordinal).getOrElse(foldInt(plan, ordinal, least = false))

  def minWideLong(plan: Plan[WideRow], ordinal: Int): Long =
    WideExtremum.minLong(plan, ordinal).getOrElse(foldLong(plan, ordinal, least = true))

  def maxWideLong(plan: Plan[WideRow], ordinal: Int): Long =
    WideExtremum.maxLong(plan, ordinal).getOrElse(foldLong(plan, ordinal, least = false))

  def minWideFloat(plan: Plan[WideRow], ordinal: Int): Float =
    WideExtremum.minFloat(plan, ordinal).getOrElse(foldFloat(plan, ordinal, least = true))

  def maxWideFloat(plan: Plan[WideRow], ordinal: Int): Float =
    WideExtremum.maxFloat(plan, ordinal).getOrElse(foldFloat(plan, ordinal, least = false))

  def minWideDouble(plan: Plan[WideRow], ordinal: Int): Double =
    WideExtremum.minDouble(plan, ordinal).getOrElse(foldDouble(plan, ordinal, least = true))

  def maxWideDouble(plan: Plan[WideRow], ordinal: Int): Double =
    WideExtremum.maxDouble(plan, ordinal).getOrElse(foldDouble(plan, ordinal, least = false))

  private def foldInt(plan: Plan[WideRow], ordinal: Int, least: Boolean): Int =
    rowPartitions(plan)
      .foldLeft((0, false)) { (state, partition) =>
        partition.data.foldLeft(state) { (acc, row) =>
          row.cells(ordinal) match
            case Cell.Int32(value) => keepInt(acc, value, least)
            case Cell.Null => acc
            case other =>
              throw new IllegalArgumentException(s"column $ordinal is $other, expected an int")
        }
      }
      ._1

  private def foldLong(plan: Plan[WideRow], ordinal: Int, least: Boolean): Long =
    rowPartitions(plan)
      .foldLeft((0L, false)) { (state, partition) =>
        partition.data.foldLeft(state) { (acc, row) =>
          row.cells(ordinal) match
            case Cell.Int64(value) => keepLong(acc, value, least)
            case Cell.Null => acc
            case other =>
              throw new IllegalArgumentException(s"column $ordinal is $other, expected a long")
        }
      }
      ._1

  private def foldFloat(plan: Plan[WideRow], ordinal: Int, least: Boolean): Float =
    rowPartitions(plan)
      .foldLeft((0.0f, false)) { (state, partition) =>
        partition.data.foldLeft(state) { (acc, row) =>
          row.cells(ordinal) match
            case Cell.Float32(value) => keepFloat(acc, value, least)
            case Cell.Null => acc
            case other =>
              throw new IllegalArgumentException(s"column $ordinal is $other, expected a float")
        }
      }
      ._1

  private def foldDouble(plan: Plan[WideRow], ordinal: Int, least: Boolean): Double =
    rowPartitions(plan)
      .foldLeft((0.0, false)) { (state, partition) =>
        partition.data.foldLeft(state) { (acc, row) =>
          row.cells(ordinal) match
            case Cell.Float64(value) => keepDouble(acc, value, least)
            case Cell.Null => acc
            case other =>
              throw new IllegalArgumentException(s"column $ordinal is $other, expected a double")
        }
      }
      ._1

  private def keepInt(state: (Int, Boolean), value: Int, least: Boolean): (Int, Boolean) =
    val (best, seen) = state
    if (!seen || (least && value < best) || (!least && value > best)) then (value, true)
    else (best, true)

  private def keepLong(state: (Long, Boolean), value: Long, least: Boolean): (Long, Boolean) =
    val (best, seen) = state
    if (!seen || (least && value < best) || (!least && value > best)) then (value, true)
    else (best, true)

  private def keepFloat(state: (Float, Boolean), value: Float, least: Boolean): (Float, Boolean) =
    val (best, seen) = state
    if (!seen || (least && value < best) || (!least && value > best)) then (value, true)
    else (best, true)

  private def keepDouble(
      state: (Double, Boolean),
      value: Double,
      least: Boolean,
  ): (Double, Boolean) =
    val (best, seen) = state
    if (!seen || (least && value < best) || (!least && value > best)) then (value, true)
    else (best, true)

  def sumDouble(plan: Plan[Double]): Double =
    ColumnarPlanner.trySumDouble(plan).getOrElse {
      rowPartitions(plan).foldLeft(0.0) { (total, partition) =>
        partition.data.foldLeft(total)((acc, value) => acc + value)
      }
    }

  def sumFloat(plan: Plan[Float]): Float =
    ColumnarPlanner.trySumFloat(plan).getOrElse {
      rowPartitions(plan).foldLeft(0.0f) { (total, partition) =>
        partition.data.foldLeft(total)((acc, value) => acc + value)
      }
    }

  def groupedSum(plan: Plan[(Int, Int)]): Long =
    ColumnarPlanner.tryGroupedSum(plan).getOrElse(groupedSumOnRows(plan))

  /**
   * Row fallback. Pair filters and pair maps are peeled like the wide path and run on present
   * cells, so a typed `(Int, Int)` pattern never unboxes a null to 0. Any other plan is executed
   * first, and its pairs are folded the same way.
   */
  private def groupedSumOnRows(plan: Plan[(Int, Int)]): Long =
    val (base, steps) = ColumnarPlanner.peelPairOps(plan)
    base match
      case source: Plan.Source[_] if ColumnarPlanner.groupedPairSource(source) =>
        foldPartitions(source.partitions, steps)
      case _ =>
        foldPartitions(rowPartitions(plan), Nil)

  private def foldPartitions(partitions: Seq[Partition[_]], steps: List[PairStep]): Long =
    partitions.foldLeft(0L)((total, partition) => addRows(partition.data, steps, total))

  /**
   * Sum of present pairs. A null component drops the row before `steps`. Any other cell type
   * fails.
   *
   * Named erasure boundary. `(Int, Int)` erases its fields. `productElement` keeps a null
   * component visible without unboxing, and the two cells are matched in place so the row does not
   * allocate. A non-null cell is a reference, so the class-name cast is safe.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def addRows(rows: Iterable[_], steps: List[PairStep], total: Long): Long =
    def reject(role: String, cell: Any): Nothing =
      val found = cell match
        case null => "null"
        case other => other.asInstanceOf[AnyRef].getClass.getName
      throw new IllegalArgumentException(s"grouped sum expected an Int $role, found $found")

    def one(row: Any): Long = row match
      case pair: Tuple2[?, ?] =>
        val key = pair.productElement(0)
        val value = pair.productElement(1)
        key match
          case null =>
            value match
              case null | _: Int => 0L
              case other => reject("value", other)
          case presentKey: Int =>
            value match
              case null => 0L
              case present: Int =>
                PairStep.valueAfter(presentKey, present, steps).fold(0L)(_.toLong)
              case other => reject("value", other)
          case other => reject("key", other)
      case other => reject("key", other)

    rows.foldLeft(total)((acc, row) => acc + one(row))

  private def rowPartitions[A](plan: Plan[A]): Seq[Partition[A]] = plan match {
    case source: Plan.Source[A] => source.partitions
    case _ => runRow(plan)
  }

  private def runRow[A](plan: Plan[A]): Seq[Partition[A]] = {
    val runtime = ScarletRuntime.get
    val scheduler = new DAGScheduler(runtime.shuffle, runtime.scheduler, runtime.partitioner)
    scheduler.executePartitions(plan)
  }

  def take[A](plan: Plan[A], n: Int): Seq[A] =
    if (n <= 0) Seq.empty
    else
      ColumnarPlanner.tryTake(plan, n).getOrElse {
        // First `n` rows of the split plan. Wrapping `it.take(n)` around `plan` first makes the
        // root opaque, and the row scheduler then publishes no child batch.
        MixedAction.partitions(plan).iterator.flatMap(_.data.iterator).take(n).toSeq
      }
}

/**
 * Registers the default execution service when the execution module is loaded. A service already
 * installed in [[com.ewoodbury.scarlet.core.ExecutionService]] is left in place.
 */
object DefaultExecutionService {

  /**
   * Installs this service only while the registry still holds its unregistered placeholder.
   * Repeated calls are idempotent.
   */
  def initialize(): Unit = {
    val _ = ExecutionService.registerIfEmpty(new DefaultExecutionService())
  }
}
