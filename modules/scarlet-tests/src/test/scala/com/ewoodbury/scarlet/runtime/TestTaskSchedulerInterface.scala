package com.ewoodbury.scarlet.runtime

import scala.concurrent.duration.*

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.{Partition, RetryPolicy}
import com.ewoodbury.scarlet.runtime.api.{RunnableTask, TaskScheduler}

class TestTaskSchedulerInterface extends AnyFlatSpec with Matchers {

  // Mock implementation for testing interface contract
  class MockTaskScheduler extends TaskScheduler {
    override def submit[A, B](tasks: Seq[RunnableTask[A, B]]): Seq[Partition[B]] =
      Seq.empty[Partition[B]]

    override def submitWithRetry[A, B](
      tasks: Seq[RunnableTask[A, B]],
      retryPolicy: RetryPolicy
    ): Seq[Partition[B]] =
      submit(tasks) // Simple implementation for testing

    override def shutdown(): Unit = ()
  }

  "TaskScheduler interface" should "support both submit methods" in {
    val scheduler = new MockTaskScheduler()

    // Test that both methods can be called (interface contract)
    val tasks = Seq.empty[RunnableTask[Int, String]]
    val retryPolicy = RetryPolicy.default

    // These should compile and not throw exceptions
    val result1 = scheduler.submit(tasks)
    val result2 = scheduler.submitWithRetry(tasks, retryPolicy)
    scheduler.shutdown()

    result1 shouldBe Seq.empty[Partition[String]]
    result2 shouldBe Seq.empty[Partition[String]]
  }

  it should "accept various retry policies" in {
    val scheduler = new MockTaskScheduler()
    val tasks = Seq.empty[RunnableTask[Int, String]]

    val policies = List(
      RetryPolicy.NoRetry,
      RetryPolicy.default,
      RetryPolicy.ExponentialBackoff(5, 2.seconds, 60.seconds),
      RetryPolicy.forExceptions(3, 1.second, 30.seconds)(classOf[RuntimeException])
    )

    // All should compile and work
    policies.foreach { policy =>
      val result = scheduler.submitWithRetry(tasks, policy)
      result shouldBe Seq.empty[Partition[String]]
    }
  }

  it should "maintain type safety across different task types" in {
    val scheduler = new MockTaskScheduler()

    // Test with different type parameters
    val intTasks = Seq.empty[RunnableTask[Int, Int]]
    val stringTasks = Seq.empty[RunnableTask[String, String]]
    val kvTasks = Seq.empty[RunnableTask[(String, Int), (String, Int)]]

    val policy = RetryPolicy.default

    // All should compile with correct types
    val intResult: Seq[Partition[Int]] = scheduler.submit(intTasks)
    val stringResult: Seq[Partition[String]] = scheduler.submit(stringTasks)
    val kvResult: Seq[Partition[(String, Int)]] = scheduler.submit(kvTasks)

    val intRetryResult: Seq[Partition[Int]] = scheduler.submitWithRetry(intTasks, policy)
    val stringRetryResult: Seq[Partition[String]] = scheduler.submitWithRetry(stringTasks, policy)
    val kvRetryResult: Seq[Partition[(String, Int)]] = scheduler.submitWithRetry(kvTasks, policy)

    intResult shouldBe Seq.empty[Partition[Int]]
    stringResult shouldBe Seq.empty[Partition[String]]
    kvResult shouldBe Seq.empty[Partition[(String, Int)]]
    intRetryResult shouldBe Seq.empty[Partition[Int]]
    stringRetryResult shouldBe Seq.empty[Partition[String]]
    kvRetryResult shouldBe Seq.empty[Partition[(String, Int)]]
  }
}
