package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/** Columnar union: concatenation of two same-kind inputs, duplicates kept. */
class TestUnion extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "a union" should "concatenate both sides and keep duplicates" in {
    val left = DistCollection(Seq(1, 2), 2)
    val right = DistCollection(Seq(2, 3), 2)
    val union = left.union(right)

    union.collect() shouldBe Seq(1, 2, 2, 3)
    ColumnarPlanner.lower(union.plan).map(_.shape) shouldBe Some("Union(Scan, Scan)")
    ColumnarPlanner.partitioning(union.plan).map(_.numPartitions) shouldBe Some(4)
  }

  it should "match the row path when both sides are empty" in {
    val left = DistCollection(Seq.empty[Int], 2)
    val right = DistCollection(Seq.empty[Int], 1)
    val union = left.union(right)

    union.collect() shouldBe empty
    // An empty int source has no witness, so classification refuses it and the union stays on rows.
    ColumnarPlanner.lower(union.plan) shouldBe Option.empty[PhysicalOp]
  }

  it should "concatenate pair inputs" in {
    val left = DistCollection(Seq(1 -> 10), 1)
    val right = DistCollection(Seq(2 -> 20), 1)
    val union = left.union(right)

    union.collect().toSeq shouldBe Seq(1 -> 10, 2 -> 20)
    ColumnarPlanner.lower(union.plan).map(_.shape) shouldBe Some("Union(Scan, Scan)")
  }
