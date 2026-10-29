package com.ewoodbury.scarlet.core

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.execution.DefaultExecutionService

/**
 * Pins the registry contract: a service installed before the first action stays installed, and a
 * later registration does not replace it.
 */
class TestExecutionServiceRegistration extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  // Captured when this suite starts. Restoring it puts the placeholder back when this suite ran
  // against an empty registry, and puts the live service back when an earlier suite installed one.
  private val original = ExecutionService.get

  override def afterEach(): Unit = {
    ExecutionService.set(original)
    super.afterEach()
  }

  "A custom execution service" should "survive initialize and the first action" in {
    val custom = new FixedExecutionService
    ExecutionService.set(custom)

    DefaultExecutionService.initialize()

    ExecutionService.get should be theSameInstanceAs custom
    DistCollection(Seq(1, 2, 3), 1).collect() shouldBe empty
    ExecutionService.get should be theSameInstanceAs custom
  }

  "DefaultExecutionService.initialize" should "leave the service installed by the first call" in {
    DefaultExecutionService.initialize()
    val first = ExecutionService.get

    DefaultExecutionService.initialize()

    ExecutionService.unregistered shouldBe false
    ExecutionService.get should be theSameInstanceAs first
  }

  "registerIfEmpty" should "leave an installed service in place" in {
    val custom = new FixedExecutionService
    ExecutionService.set(custom)

    ExecutionService.registerIfEmpty(new FixedExecutionService) shouldBe false
    ExecutionService.get should be theSameInstanceAs custom
  }

  /**
   * Returns no rows, so a `collect` is distinguishable from the default service without depending
   * on the plan.
   */
  private final class FixedExecutionService extends ExecutionService {

    def execute[A](plan: Plan[A]): Seq[A] = Seq.empty[A]

    def executePartitions[A](plan: Plan[A]): Seq[Partition[A]] = Seq.empty

    def count[A](plan: Plan[A]): Long = 0L

    def sumInt(plan: Plan[Int]): Long = 0L

    def sumLong(plan: Plan[Long]): Long = 0L

    def sumDouble(plan: Plan[Double]): Double = 0.0

    def groupedSum(plan: Plan[(Int, Int)]): Long = 0L

    def take[A](plan: Plan[A], n: Int): Seq[A] = Seq.empty[A]
  }
}
