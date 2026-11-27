package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.GroupByKey
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * `minByKey` and `maxByKey` against the row path.
 *
 * A null key is dropped. A null value is skipped. A key whose values are all null emits no row.
 * `groupByKey` is not this operator.
 */
class TestExtremum extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 8192,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
        spillEnabled = false,
      ),
    )
    ScarletRuntime.get.shuffle.clear()
    GroupByKey.resetValueListsBuilt()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "minByKey" should "match the row fold across partitions" in {
    val data = pairs()
    GroupByKey.resetValueListsBuilt()
    ColumnarPlanner.lower(data.minByKey.plan).map(_.shape) shouldBe Some(
      "IntExtremum(Exchange(Scan),min)",
    )
    data.minByKey.collect().toMap shouldBe Map(1 -> 3, 2 -> -4, 3 -> 0)
    GroupByKey.valueListsBuilt shouldBe 0L
    onRows(data.minByKey.collect().toMap) shouldBe Map(1 -> 3, 2 -> -4, 3 -> 0)
  }

  "maxByKey" should "match the row fold across partitions" in {
    val data = pairs()
    ColumnarPlanner.lower(data.maxByKey.plan).map(_.shape) shouldBe Some(
      "IntExtremum(Exchange(Scan),max)",
    )
    data.maxByKey.collect().toMap shouldBe Map(1 -> 9, 2 -> 8, 3 -> 0)
    onRows(data.maxByKey.collect().toMap) shouldBe Map(1 -> 9, 2 -> 8, 3 -> 0)
  }

  it should "keep the extreme int" in {
    val data = DistCollection.ofIntPairs(
      Array(1, 1, 2, 2),
      Array(Int.MinValue, 1, Int.MaxValue, -1),
      2,
    )
    data.minByKey.collect().toMap shouldBe Map(1 -> Int.MinValue, 2 -> -1)
    data.maxByKey.collect().toMap shouldBe Map(1 -> 1, 2 -> Int.MaxValue)
  }

  it should "drop a null key and skip a null value" in {
    val data = nullPairs()
    data.minByKey.collect().toMap shouldBe Map(1 -> 4, 5 -> 1)
    data.maxByKey.collect().toMap shouldBe Map(1 -> 5, 5 -> 3)
    onRows(data.minByKey.collect().toMap) shouldBe Map(1 -> 4, 5 -> 1)
    onRows(data.maxByKey.collect().toMap) shouldBe Map(1 -> 5, 5 -> 3)
    data.groupByKey.plan match
      case Plan.GroupByKeyOp(_) => succeed
      case other => fail(s"groupByKey became $other")
  }

  private def pairs(): DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(
      Array(1, 2, 1, 3, 2, 1),
      Array(5, 8, 3, 0, -4, 9),
      2,
    )

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullPairs(): DistCollection[(Int, Int)] =
    def cell(key: Any, value: Any): (Any, Any) = (key, value)
    val source = Plan.Source(
      Seq(
        Partition(Seq(cell(1, 4), cell(null, 9), cell(5, 1), cell(1, 5), cell(7, null))),
        Partition(Seq(cell(5, 3), cell(2, null), cell(null, 8), cell(7, null))),
      ),
    )
    DistCollection(source.asInstanceOf[Plan[(Int, Int)]])

  private def onRows[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
