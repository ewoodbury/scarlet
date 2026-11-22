package com.ewoodbury.scarlet.execution

import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.runtime.api.{Partitioner, ShuffleService, TaskScheduler}

/**
 * The single execution entry point: compiles a plan into a stage graph and runs its stages in
 * topological order, coordinating shuffles between them.
 */
final class DAGScheduler(
    shuffle: ShuffleService,
    scheduler: TaskScheduler,
    partitioner: Partitioner,
) extends StrictLogging {

  private val joinExecutor = new JoinExecutor(shuffle, scheduler)
  private val shuffleHandler = new ShuffleHandler(shuffle, partitioner)
  private val stageExecutor = new StageExecutor(shuffle, scheduler, joinExecutor)
  private val executionPlanner = new ExecutionPlanner(stageExecutor, shuffleHandler)

  /**
   * Executes a plan using multi-stage execution, handling shuffle boundaries.
   */
  def execute[A](plan: Plan[A]): Iterable[A] =
    executePartitions(plan).flatMap(_.data)

  /**
   * Executes a plan and returns the final stage's output with partition boundaries preserved.
   * Callers should flatten only at the outermost boundary so partition-aware consumers (such as
   * aggregate) can operate per partition.
   *
   * Named erasure boundary: stage results are stored as `Partition[_]`; the final stage produced
   * plan type A by construction of the graph, so the cast is safe.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def executePartitions[A](plan: Plan[A]): Seq[Partition[A]] = {
    val (stageGraph, stageResults) = runStages(plan)
    stageResults(stageGraph.finalStageId).asInstanceOf[Seq[Partition[A]]]
  }

  /**
   * Runs all stages in topological order and returns the results of every stage, keyed by stage
   * ID.
   */
  private def runStages[A](
      plan: Plan[A],
  ): (StageBuilder.StageGraph, Map[StageId, Seq[Partition[_]]]) = {
    logger.info("DAGScheduler: starting multi-stage execution")
    val stageGraph = StageBuilder.buildStageGraph(plan)
    logger.debug(s"DAGScheduler: built stage graph with ${stageGraph.stages.size} stages")
    val executionOrder = TopologicalSort.sort(stageGraph.stages.keySet, stageGraph.dependencies)
    logger.debug(
      s"DAGScheduler: execution order: ${executionOrder.map(_.toInt).mkString(" -> ")}",
    )
    val stageResults = executionPlanner.runStages(stageGraph, executionOrder)
    logger.info("DAGScheduler: multi-stage execution completed")
    (stageGraph, stageResults)
  }
}
