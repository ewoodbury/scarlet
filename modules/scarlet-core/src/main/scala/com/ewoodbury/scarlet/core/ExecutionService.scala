package com.ewoodbury.scarlet.core

/**
 * Service Provider Interface for executing plans. This allows the API module to trigger execution
 * without depending on the execution module.
 */
trait ExecutionService {

  /**
   * Executes a plan and returns the results.
   */
  def execute[A](plan: Plan[A]): Seq[A]

  /**
   * Executes a plan and returns the results partitioned as the engine produced them. Preserving
   * partition boundaries enables partition-aware actions such as aggregate.
   */
  def executePartitions[A](plan: Plan[A]): Seq[Partition[A]]

  /**
   * Executes a plan and returns the count of results.
   */
  def count[A](plan: Plan[A]): Long

  /**
   * Sums an `Int` plan as a `Long`. Does not wrap. Empty input returns zero.
   */
  def sumInt(plan: Plan[Int]): Long

  /**
   * Sums a `Long` plan. Wraps. Empty input returns zero.
   */
  def sumLong(plan: Plan[Long]): Long

  /**
   * Sums a `Double` plan. Empty input returns zero.
   */
  def sumDouble(plan: Plan[Double]): Double

  /**
   * Sums `(Int, Int)` values by key as a `Long` and folds the group sums. Empty input returns
   * zero. A null component drops the row before a fused filter or map. Any other cell type fails.
   * The total does not wrap at `Int`.
   */
  def groupedSum(plan: Plan[(Int, Int)]): Long

  /**
   * Executes a plan and returns the first n results.
   */
  def take[A](plan: Plan[A], n: Int): Seq[A]
}

/**
 * Global registry for the execution service. The execution module will register an implementation
 * here.
 */
object ExecutionService {
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var current: ExecutionService = new ExecutionService {
    def execute[A](plan: Plan[A]): Seq[A] = {
      throw new UnsupportedOperationException(
        "No execution service registered. Please ensure the execution module is properly initialized.",
      )
    }

    def executePartitions[A](plan: Plan[A]): Seq[Partition[A]] = {
      throw new UnsupportedOperationException(
        "No execution service registered. Please ensure the execution module is properly initialized.",
      )
    }

    def count[A](plan: Plan[A]): Long = {
      throw new UnsupportedOperationException(
        "No execution service registered. Please ensure the execution module is properly initialized.",
      )
    }

    def sumInt(plan: Plan[Int]): Long = {
      throw new UnsupportedOperationException(
        "No execution service registered. Please ensure the execution module is properly initialized.",
      )
    }

    def sumLong(plan: Plan[Long]): Long = {
      throw new UnsupportedOperationException(
        "No execution service registered. Please ensure the execution module is properly initialized.",
      )
    }

    def sumDouble(plan: Plan[Double]): Double = {
      throw new UnsupportedOperationException(
        "No execution service registered. Please ensure the execution module is properly initialized.",
      )
    }

    def groupedSum(plan: Plan[(Int, Int)]): Long = {
      throw new UnsupportedOperationException(
        "No execution service registered. Please ensure the execution module is properly initialized.",
      )
    }

    def take[A](plan: Plan[A], n: Int): Seq[A] = {
      throw new UnsupportedOperationException(
        "No execution service registered. Please ensure the execution module is properly initialized.",
      )
    }
  }

  def get: ExecutionService = current

  def set(service: ExecutionService): Unit = {
    current = service
  }
}
