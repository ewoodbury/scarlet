package com.ewoodbury.scarlet.execution

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import com.ewoodbury.scarlet.core.{ExecutionService, Partition, Plan}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Bridges the API to the execution engine. A bare source returns its partitions. Plans that
 * [[ColumnarPlanner]] accepts run on the columnar kernels. Every other plan is compiled into a
 * stage graph and executed through the DAGScheduler.
 */
class DefaultExecutionService extends ExecutionService {

  def execute[A](plan: Plan[A]): Seq[A] =
    executePartitions(plan).flatMap(_.data)

  def executePartitions[A](plan: Plan[A]): Seq[Partition[A]] = plan match {
    case s: Plan.Source[A] =>
      // A bare source needs no tasks; its partitions are the result
      s.partitions

    case _ =>
      ColumnarPlanner.tryRun(plan).getOrElse(runRow(plan))
  }

  def count[A](plan: Plan[A]): Long =
    ColumnarPlanner.tryCount(plan).getOrElse {
      rowPartitions(plan).foldLeft(0L)((total, partition) => total + partition.data.size)
    }

  def sumInt(plan: Plan[Int]): Long =
    ColumnarPlanner.trySumInt(plan).getOrElse {
      rowPartitions(plan).foldLeft(0L) { (total, partition) =>
        partition.data.foldLeft(total)((acc, value) => acc + value.toLong)
      }
    }

  def sumDouble(plan: Plan[Double]): Double =
    ColumnarPlanner.trySumDouble(plan).getOrElse {
      rowPartitions(plan).foldLeft(0.0) { (total, partition) =>
        partition.data.foldLeft(total)((acc, value) => acc + value)
      }
    }

  def groupedSum(plan: Plan[(Int, Int)]): Long =
    ColumnarPlanner.tryGroupedSum(plan).getOrElse {
      rowPartitions(plan).foldLeft(0L) { (total, partition) =>
        partition.data.foldLeft(total)((acc, pair) => acc + presentValue(pair))
      }
    }

  /**
   * Present key and present value, widened to `Long`. A null key drops the row. A null value
   * contributes zero. Any other cell type fails. That is the folded total of `sumByKey`: an absent
   * key never contributes, and an absent value does not change the sum.
   *
   * Named erasure boundary. `(Int, Int)` erases its fields. `productElement` keeps a null
   * component visible without unboxing, and the two cells are matched in place so the row does not
   * allocate a pair. A non-null cell is a reference, so the class-name cast is safe.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def presentValue(pair: (Int, Int)): Long =
    def reject(role: String, cell: Any): Nothing =
      val found = cell.asInstanceOf[AnyRef].getClass.getName
      throw new IllegalArgumentException(s"grouped sum expected an Int $role, found $found")

    val key = pair.productElement(0)
    val value = pair.productElement(1)
    key match
      case null =>
        value match
          case null | _: Int => 0L
          case other => reject("value", other)
      case _: Int =>
        value match
          case null => 0L
          case present: Int => present.toLong
          case other => reject("value", other)
      case other => reject("key", other)

  private def rowPartitions[A](plan: Plan[A]): Seq[Partition[A]] = plan match {
    case source: Plan.Source[A] => source.partitions
    case _ => runRow(plan)
  }

  private def runRow[A](plan: Plan[A]): Seq[Partition[A]] = {
    val runtime = ScarletRuntime.get
    val scheduler = new DAGScheduler[IO](runtime.shuffle, runtime.scheduler, runtime.partitioner)
    scheduler.executePartitions(plan).unsafeRunSync()
  }

  def take[A](plan: Plan[A], n: Int): Seq[A] =
    if (n <= 0) Seq.empty
    else
      ColumnarPlanner.tryTake(plan, n).getOrElse {
        // Truncate each output partition of the final stage to n elements before executing. Each
        // partition can then contribute at most n elements, so the first n elements of the
        // partition-ordered result are unchanged.
        val limited = Plan.MapPartitionsOp(plan, (it: Iterator[A]) => it.take(n))
        executePartitions(limited).flatMap(_.data).take(n)
      }
}

/**
 * Auto-registration of the default execution service. This gets called when the execution module
 * is loaded.
 */
object DefaultExecutionService {
  // Register the service when this object is first accessed
  private lazy val registration = {
    ExecutionService.set(new DefaultExecutionService())
  }

  // Ensure the registration happens by calling this method
  def initialize(): Unit = {
    // Force the lazy initialization to run
    registration
  }
}
