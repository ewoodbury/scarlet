package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.PairStep
import com.ewoodbury.scarlet.core.{ExecutionService, Partition, Plan}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Bridges the API to the execution engine. A bare source returns its partitions. Plans that
 * [[ColumnarPlanner]] accepts run on the columnar kernels. A `mapPartitions` tail runs that child
 * first, then the row function on the decoded partitions. A child the planner refuses publishes
 * nothing. A join with one lowered side decodes that side and runs the join on the row executor
 * when those rows are the rows that executor produces. Every other refused plan is compiled into a
 * stage graph and executed through the DAGScheduler.
 *
 * `take` of a fully bound sort stays on [[ColumnarPlanner.tryTake]]. `count` of a fully bound plan
 * stays on [[ColumnarPlanner.tryCount]]. A miss splits a `mapPartitions` tail or a one-sided join
 * before any limit: the columnar child publishes, then the tail is limited or counted. `flatMap`
 * and untyped `sortBy` have no split, so they publish nothing. A [[ColumnarFallback]] from a
 * kernel still reruns that whole plan on rows.
 */
class DefaultExecutionService extends ExecutionService {

  def execute[A](plan: Plan[A]): Seq[A] =
    executePartitions(plan).flatMap(_.data)

  def executePartitions[A](plan: Plan[A]): Seq[Partition[A]] = plan match {
    case s: Plan.Source[A] =>
      // A bare source needs no tasks; its partitions are the result
      s.partitions

    case Plan.MapPartitionsOp(source, f) =>
      MixedTail.partitions(executePartitions(source), f)

    case _ =>
      ColumnarPlanner.tryRun(plan).orElse(MixedJoin.partitions(plan)).getOrElse(runRow(plan))
  }

  def count[A](plan: Plan[A]): Long =
    ColumnarPlanner.tryCount(plan).getOrElse {
      splitPlan(plan).map(sized).getOrElse(sized(rowPartitions(plan)))
    }

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

  def sumDouble(plan: Plan[Double]): Double =
    ColumnarPlanner.trySumDouble(plan).getOrElse {
      rowPartitions(plan).foldLeft(0.0) { (total, partition) =>
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
    else ColumnarPlanner.tryTake(plan, n).getOrElse(takeUnbound(plan, n))

  /**
   * A refused plan after its columnar child has already run.
   *
   * [[Plan.MapPartitionsOp]] runs the child, then the row function. A join with one lowered side
   * is [[MixedJoin]]. `None` is a plan with nothing to split: a fully bound plan whose take or
   * count kernel declined, `flatMap`, or untyped `sortBy`. The limit is applied by the caller, so
   * this does not wrap `take` around the original plan.
   */
  private def splitPlan[A](plan: Plan[A]): Option[Seq[Partition[A]]] = plan match {
    case _: Plan.MapPartitionsOp[?, ?] =>
      Some(executePartitions(plan))
    case other =>
      if (ColumnarPlanner.lower(other).isDefined) None
      else MixedJoin.partitions(other)
  }

  /** Row count of partitions a split or the row executor already produced. */
  private def sized(parts: Seq[Partition[_]]): Long =
    parts.foldLeft(0L)((total, partition) => total + partition.data.size)

  /**
   * `take` when [[ColumnarPlanner.tryTake]] declines. A split runs first. The row wrapper is only
   * the plan [[splitPlan]] refuses, and it stays on the row scheduler so a bound aggregate is not
   * taken through [[executePartitions]].
   */
  private def takeUnbound[A](plan: Plan[A], n: Int): Seq[A] =
    splitPlan(plan).map(parts => prefix(parts, n)).getOrElse(rowTake(plan, n))

  /**
   * First `n` elements in partition order, after the split. Each partition contributes at most
   * `n`, so the prefix matches a per-partition limit followed by a global one.
   */
  private def prefix[A](parts: Seq[Partition[A]], n: Int): Seq[A] =
    parts.iterator.flatMap(partition => partition.data.iterator.take(n)).take(n).toList

  /**
   * Row `take`. The wrapper limits each output partition before the stage runs. It is not applied
   * to a plan [[splitPlan]] accepts.
   */
  private def rowTake[A](plan: Plan[A], n: Int): Seq[A] = {
    val limited = Plan.MapPartitionsOp(plan, (it: Iterator[A]) => it.take(n))
    runRow(limited).flatMap(_.data).take(n)
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
