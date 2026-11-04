package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/** Columnar distinct: each value once, whatever the partition count. */
class TestDistinct extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  private def columnar(values: Seq[Int], partitions: Int): Seq[Int] =
    DistCollection(values, partitions).distinct().collect().toSeq.sorted

  // The row path runs distinct per partition, so duplicates that land in different partitions
  // survive. That is a row-path bug tracked in RETIRE-ROW.md; the columnar path is global.

  "a columnar distinct" should "keep one of each value across partitions" in {
    // Values 1 and 4 sit in different source partitions, so a per-partition dedupe would keep
    // both copies. Global distinct must return each once.
    val values = Seq(1, 2, 2, 3, 1, 4, 4, 4, 0, -1)
    columnar(values, 3) shouldBe Seq(-1, 0, 1, 2, 3, 4)
    ColumnarPlanner.lower(DistCollection(values, 3).distinct().plan).map(_.shape) shouldBe
      Some("Dedupe(Exchange(Scan))")
  }

  it should "dedupe with one partition and with a prime count" in {
    val values = (1 to 100).map(_ % 7) ++ Seq(0, 0, 0)
    columnar(values, 1) shouldBe Seq(0, 1, 2, 3, 4, 5, 6)
    columnar(values, 5) shouldBe Seq(0, 1, 2, 3, 4, 5, 6)
    columnar(values, 7) shouldBe Seq(0, 1, 2, 3, 4, 5, 6)
  }

  it should "return empty for empty input" in {
    columnar(Seq.empty, 3) shouldBe empty
  }

  it should "keep every value when all are distinct" in {
    val values = (0 until 500).toSeq
    columnar(values, 4).length shouldBe 500
  }

  it should "refuse take as not partition-preserving" in {
    val lowered = ColumnarPlanner.lower(DistCollection(Seq(1, 2, 2), 2).distinct().plan).get
    lowered.shape shouldBe "Dedupe(Exchange(Scan))"
    PartitionPreserving.of(lowered) shouldBe Option.empty[Int]
  }
