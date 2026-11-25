package com.ewoodbury.scarlet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.execution.schema.{Describe, KernelChoice}

/**
 * The stored tag is the ledger label.
 *
 * A hash aggregate whose child schema is `[Int32, Int64]` and whose stored tag is
 * [[PhysicalOp.HashAggregateTag.IntHash]] describes as [[KernelChoice.IntHashAggregate]].
 * [[PhysicalSchema.isIntLongAggregate]] stays true. Naming the aggregate from that schema test
 * would report [[KernelChoice.IntLongProjectSum]]. [[PhysicalOp.shape]] ignores the tag.
 */
class TestStoredAggregateTag extends AnyFlatSpec with Matchers:

  private def intLongChild: PhysicalOp =
    PhysicalOp.Exchange(PhysicalOp.Project(PhysicalOp.Scan(PhysicalOp.Kind.Triples, Seq.empty)))

  "a stored IntHash tag" should "describe as IntHashAggregate on an int-long child" in {
    val aggregate = PhysicalOp.HashAggregate(intLongChild, PhysicalOp.HashAggregateTag.IntHash)
    PhysicalSchema.isIntLongAggregate(aggregate) shouldBe true
    Describe.tag(aggregate) shouldBe KernelChoice.IntHashAggregate
    aggregate.shape shouldBe "HashAggregate(Exchange(Project(Scan)))"
    ExecutionRoute.countArm(aggregate) shouldBe ExecutionRoute.CountArm.IntLong
  }

  "a stored IntLongSum tag" should "describe as IntLongProjectSum on the same child" in {
    val aggregate =
      PhysicalOp.HashAggregate(intLongChild, PhysicalOp.HashAggregateTag.IntLongSum)
    PhysicalSchema.isIntLongAggregate(aggregate) shouldBe true
    Describe.tag(aggregate) shouldBe KernelChoice.IntLongProjectSum
    aggregate.shape shouldBe "HashAggregate(Exchange(Project(Scan)))"
  }
