package com.ewoodbury.scarlet.execution

import scala.collection.mutable

import com.ewoodbury.scarlet.core.StageId

import StageBuilder.{ShuffleInput, Side, StageGraph}

/**
 * Checks a finished stage graph before the scheduler runs it. Construction stays in StageBuilder.
 */
@SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
object StageGraphValidation:

  private[execution] def validate(graph: StageGraph): Unit = {
    // 1. finalStageId exists in stages map
    if (!graph.stages.contains(graph.finalStageId)) {
      val availableStages = graph.stages.keys.toSeq.sorted.mkString(", ")
      throw new IllegalStateException(
        s"Final stage ID ${graph.finalStageId} not found in stages map. " +
          s"Available stage IDs: [$availableStages]. " +
          "This indicates a stage graph construction error.",
      )
    }

    // 2. Every dependency target exists
    graph.dependencies.foreachEntry { (stageId, deps) =>
      if (!graph.stages.contains(stageId)) {
        val availableStages = graph.stages.keys.toSeq.sorted.mkString(", ")
        throw new IllegalStateException(
          s"Stage $stageId has dependencies but is not in stages map. " +
            s"Available stage IDs: [$availableStages]. " +
            s"Dependencies: [${deps.mkString(", ")}]",
        )
      }
      deps.foreach { depId =>
        if (!graph.stages.contains(depId)) {
          val availableStages = graph.stages.keys.toSeq.sorted.mkString(", ")
          throw new IllegalStateException(
            s"Stage $stageId depends on missing stage $depId. " +
              s"Available stage IDs: [$availableStages]. " +
              "Check stage construction order.",
          )
        }
      }
    }

    // 3. No stage lists itself as dependency (prevents infinite loops)
    graph.dependencies.foreachEntry { (stageId, deps) =>
      if (deps.contains(stageId)) {
        throw new IllegalStateException(
          s"Stage $stageId lists itself as a dependency, creating a self-cycle. " +
            s"All dependencies: [${deps.mkString(", ")}]. " +
            "This would cause infinite recursion during execution.",
        )
      }
    }

    // 4. Acyclicity check using DFS
    validateAcyclicity(graph)

    // 5. Reachability check - all stages must be reachable from finalStageId
    validateReachability(graph)

    // 6. Stage ID monotonicity check (strictly increasing sequence)
    validateStageIdMonotonicity(graph)

    // 7. Shuffle stage specific validations
    validateShuffleStages(graph)

    // 8. Partitioning metadata consistency
    graph.stages.values.foreach { stageInfo =>
      stageInfo.outputPartitioning.foreach { partitioning =>
        partitioning.invalidReason match {
          case Some(reason) =>
            throw new IllegalStateException(
              s"Stage ${stageInfo.id} has invalid partitioning: $reason",
            )
          case None => ()
        }
      }
    }
  }

  /**
   * Validates that the stage graph is acyclic using depth-first search.
   */
  private def validateAcyclicity(graph: StageGraph): Unit = {
    val visiting = mutable.Set[StageId]()
    val visited = mutable.Set[StageId]()

    def dfsVisit(stageId: StageId): Unit = {
      if (visiting.contains(stageId)) {
        val cyclePath = visiting.toSeq :+ stageId
        throw new IllegalStateException(
          s"Cycle detected in stage graph involving stage $stageId. " +
            s"Cycle path: ${cyclePath.mkString(" -> ")}. " +
            "This indicates a circular dependency that would prevent execution.",
        )
      }
      if (visited.contains(stageId)) {
        return
      }

      visiting.add(stageId)
      graph.dependencies.getOrElse(stageId, Set.empty).foreach(dfsVisit)
      visiting.remove(stageId)
      visited.add(stageId)
    }

    graph.stages.keys.foreach { stageId =>
      if (!visited.contains(stageId)) {
        dfsVisit(stageId)
      }
    }
  }

  /**
   * Validates that all stages are reachable from the final stage via reverse traversal.
   */
  private def validateReachability(graph: StageGraph): Unit = {
    val reachable = mutable.Set[StageId]()
    val toVisit = mutable.Queue[StageId]()

    // Start from finalStageId and traverse backwards through dependencies
    toVisit.enqueue(graph.finalStageId)
    reachable.add(graph.finalStageId)

    while (toVisit.nonEmpty) {
      val current = toVisit.dequeue()
      graph.dependencies.getOrElse(current, Set.empty).foreach { depId =>
        if (!reachable.contains(depId)) {
          reachable.add(depId)
          toVisit.enqueue(depId)
        }
      }
    }

    // Check for orphaned stages
    val allStageIds = graph.stages.keySet
    val orphaned = allStageIds -- reachable
    if (orphaned.nonEmpty) {
      val reachableStages = reachable.toSeq.sorted.mkString(", ")
      throw new IllegalStateException(
        s"Orphaned stages not reachable from finalStageId ${graph.finalStageId}: [${orphaned.toSeq.sorted.mkString(", ")}]. " +
          s"Reachable stages: [$reachableStages]. " +
          "Check for disconnected stage subgraphs or missing dependencies.",
      )
    }
  }

  /**
   * Validates that stage IDs form a monotonic sequence (strictly increasing from 0).
   */
  private def validateStageIdMonotonicity(graph: StageGraph): Unit = {
    val stageIds = graph.stages.keys.toSeq.sorted
    if (stageIds.nonEmpty) {
      // Check starts from 0
      stageIds.headOption match {
        case Some(firstId) if firstId.toInt != 0 =>
          throw new IllegalStateException(
            s"Stage IDs should start from 0, but found minimum ID: ${firstId.toInt}",
          )
        case None =>
          // This case shouldn't happen since we check nonEmpty above, but handle defensively
          throw new IllegalStateException("Stage ID sequence is unexpectedly empty")
        case Some(_) => // firstId.toInt == 0, which is correct
      }

      // Check for gaps (warn only to future-proof ID reuse scenarios)
      val expectedSequence = (0 until stageIds.length).map(StageId(_))
      val actualSet = stageIds.toSet
      val missing = expectedSequence.filterNot(actualSet.contains)
      if (missing.nonEmpty) {
        // Use println instead of logging to avoid dependencies
        println(
          s"Warning: Stage ID sequence has gaps. Missing IDs: ${missing.map(_.toInt).mkString(", ")}",
        )
      }
    }
  }

  /**
   * Validates shuffle stage specific invariants.
   */
  private def validateShuffleStages(graph: StageGraph): Unit = {
    graph.stages.values.foreach { stageInfo =>
      if (stageInfo.isShuffleStage) {
        // Shuffle stages should have shuffle operation metadata
        if (stageInfo.wideOp.isEmpty) {
          throw new IllegalStateException(
            s"Shuffle stage ${stageInfo.id} has no shuffle operation metadata",
          )
        }

        // Shuffle stages should have empty ops vector (they don't execute operations)
        stageInfo.stage match {
          case _: Stage.ChainedStage[_, _, _] =>
            // ChainedStage should not be used for shuffle stages
            throw new IllegalStateException(
              s"Shuffle stage ${stageInfo.id} incorrectly uses ChainedStage",
            )
          case _ => // Other stage types are acceptable for shuffle stages
        }

        // Multi-input shuffle stages should have proper side markers
        if (stageInfo.inputSources.length == 2) {
          val shuffleInputs = stageInfo.inputSources.collect { case si: ShuffleInput => si }
          if (shuffleInputs.length != 2) {
            throw new IllegalStateException(
              s"Multi-input shuffle stage ${stageInfo.id} should have exactly 2 ShuffleInputs, found ${shuffleInputs.length}",
            )
          }
          if (shuffleInputs.exists(_.side.isEmpty)) {
            throw new IllegalStateException(
              s"Multi-input shuffle stage ${stageInfo.id} has ShuffleInputs without side markers",
            )
          }
          val sides = shuffleInputs.flatMap(_.side).toSet
          if (sides.size != 2 || !sides.contains(Side.Left) || !sides.contains(Side.Right)) {
            throw new IllegalStateException(
              s"Multi-input shuffle stage ${stageInfo.id} has invalid side markers: expected {Left, Right}, found $sides",
            )
          }

          // Validate numPartitions consistency across inputs
          val numPartitionsList = shuffleInputs.map(_.numPartitions).distinct
          if (numPartitionsList.length > 1) {
            throw new IllegalStateException(
              s"Multi-input shuffle stage ${stageInfo.id} has mismatched numPartitions across inputs: ${numPartitionsList.mkString(", ")}",
            )
          }
        }
      }
    }
  }
