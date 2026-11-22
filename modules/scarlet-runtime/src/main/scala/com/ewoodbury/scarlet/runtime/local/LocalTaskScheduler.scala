package com.ewoodbury.scarlet.runtime.local

import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.scarlet.core.{Partition, RetryPolicy, ScarletConf}
import com.ewoodbury.scarlet.runtime.TaskExecutionWrapper
import com.ewoodbury.scarlet.runtime.api.{RunnableTask, TaskScheduler}

/**
 * Local implementation of the task scheduler.
 *
 * Tasks run on a fixed pool of `parallelism` threads. Each submission collects `Future`s in input
 * order and waits on them in that order. Normal submission reads the retry policy from
 * [[ScarletConf]] at submission time, so configuration changes apply to subsequent submissions; an
 * explicit policy override is available through `submitWithRetry`.
 */
final class LocalTaskScheduler(
    parallelism: Int,
) extends TaskScheduler
    with StrictLogging:

  // Daemon workers: this scheduler is process-lifetime on ScarletRuntime, and a non-daemon
  // pool would keep the JVM alive after the last non-daemon thread finishes.
  private val pool: ExecutorService =
    Executors.newFixedThreadPool(parallelism, LocalTaskScheduler.threads)

  /**
   * Submits tasks and evaluates them in parallel, respecting the configured parallelism. Failed
   * tasks are retried according to the retry policy derived from the active [[ScarletConf]].
   */
  def submit[A, B](tasks: Seq[RunnableTask[A, B]]): Seq[Partition[B]] = {
    val retryPolicy = ScarletConf.get.retryPolicy
    logger.debug(
      s"LocalTaskScheduler: submitting ${tasks.length} tasks with parallelism=$parallelism " +
        s"and maxRetries=${retryPolicy.maxRetries}",
    )
    // Logged on success and on failure.
    try executeWithWrapper(tasks, TaskExecutionWrapper.withRetryPolicy(retryPolicy))
    finally logger.debug("LocalTaskScheduler: all tasks completed")
  }

  /**
   * Submits tasks with an explicit retry policy that overrides the configured one.
   */
  def submitWithRetry[A, B](
      tasks: Seq[RunnableTask[A, B]],
      policy: RetryPolicy,
  ): Seq[Partition[B]] = {
    logger.debug(
      s"LocalTaskScheduler: submitting ${tasks.length} tasks with explicit retry policy (maxRetries=${policy.maxRetries})",
    )
    executeWithWrapper(tasks, TaskExecutionWrapper.withRetryPolicy(policy))
  }

  /**
   * Execute tasks with the given wrapper, preserving input order in the results and bounding
   * concurrency at `parallelism`.
   */
  private def executeWithWrapper[A, B](
      tasks: Seq[RunnableTask[A, B]],
      wrapper: TaskExecutionWrapper,
  ): Seq[Partition[B]] = {
    val futures: Seq[Future[Partition[B]]] = tasks.toVector.map { task =>
      pool.submit(new Callable[Partition[B]] {
        def call(): Partition[B] = wrapper.executeWithRetry(task)
      })
    }
    futures.map(future => LocalTaskScheduler.await(future))
  }

  /** Stops accepting tasks. Work already submitted is left to finish. */
  def shutdown(): Unit = pool.shutdown()

object LocalTaskScheduler:
  private val threadIds = new AtomicInteger(0)

  private val threads: ThreadFactory = (runnable: Runnable) => {
    val thread = new Thread(runnable, s"scarlet-task-${threadIds.incrementAndGet()}")
    thread.setDaemon(true)
    thread
  }

  private def await[B](future: Future[Partition[B]]): Partition[B] =
    try future.get()
    catch {
      case execution: ExecutionException =>
        throw Option(execution.getCause).getOrElse(execution)
      case interrupted: InterruptedException =>
        Thread.currentThread().interrupt()
        throw interrupted
    }
