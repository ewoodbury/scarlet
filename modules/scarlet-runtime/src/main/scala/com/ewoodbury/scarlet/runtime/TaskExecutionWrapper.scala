package com.ewoodbury.scarlet.runtime

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.scarlet.core.{Partition, RetryPolicy}
import com.ewoodbury.scarlet.runtime.api.RunnableTask

/**
 * Wrapper that executes tasks with retry logic. Handles transient failures with exponential
 * backoff and propagates permanent failures to the caller.
 */
class TaskExecutionWrapper private (
    retryPolicy: RetryPolicy,
) extends StrictLogging {

  /**
   * Execute a task with retry logic.
   *
   * @param task
   *   The task to execute
   * @param taskId
   *   Identifier for the task used in logging
   * @tparam A
   *   Input type
   * @tparam B
   *   Output type
   * @return
   *   The partition result, or throws when the policy stops retrying
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def executeWithRetry[A, B](
      task: RunnableTask[A, B],
      taskId: String = "unknown",
  ): Partition[B] = {
    var attempt = 1
    var result: Option[Partition[B]] = None
    while (result.isEmpty) {
      logger.debug(s"Executing task $taskId (attempt $attempt)")
      try {
        val partition = task.run()
        logger.debug(s"Task $taskId succeeded on attempt $attempt")
        result = Some(partition)
      } catch {
        case exception if NonFatal(exception) =>
          logger.warn(s"Task $taskId failed on attempt $attempt", exception)
          if (retryPolicy.shouldRetry(attempt, exception)) {
            val delay = retryPolicy.delayBeforeRetry(attempt)
            logger.info(
              s"Retrying task $taskId in ${delay.toMillis}ms " +
                s"(attempt ${attempt + 1}/${retryPolicy.maxRetries + 1})",
            )
            TaskExecutionWrapper.sleep(delay)
            attempt += 1
          } else {
            logger.error(s"Task $taskId failed permanently after $attempt attempts")
            throw exception
          }
      }
    }
    result match {
      case Some(partition) => partition
      case None =>
        throw new IllegalStateException("retry loop exited without a result")
    }
  }
}

object TaskExecutionWrapper {

  /**
   * Create a task execution wrapper with the given retry policy.
   */
  def withRetryPolicy(retryPolicy: RetryPolicy): TaskExecutionWrapper =
    new TaskExecutionWrapper(retryPolicy)

  /**
   * Create a default TaskExecutionWrapper using the default retry policy.
   */
  def default: TaskExecutionWrapper =
    withRetryPolicy(RetryPolicy.default)

  /**
   * Create a TaskExecutionWrapper with no retries.
   */
  def noRetries: TaskExecutionWrapper =
    withRetryPolicy(RetryPolicy.NoRetry)

  /** `Thread.sleep` clears the interrupt flag. Put it back so the caller still observes it. */
  @SuppressWarnings(Array("org.wartremover.warts.ThreadSleep"))
  private def sleep(delay: FiniteDuration): Unit =
    try Thread.sleep(delay.toMillis)
    catch {
      case interrupted: InterruptedException =>
        Thread.currentThread().interrupt()
        throw interrupted
    }
}
