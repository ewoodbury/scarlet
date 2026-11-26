package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * `countByKey` after `mapValues` to long hashes the int key. The long value is not read.
 *
 * The fake leaves `lower` empty. A filter, a `groupByKey`, and a non-adding `reduceByKey` above
 * the widening stay empty. A null key is dropped. The value function does not run.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Null",
  ),
)
class TestIntLongCount extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  it should "count keys after a value widens to long" in {
    val calls = new AtomicInteger
    val widen: Int => Long = value => {
      val _ = calls.incrementAndGet()
      if (value == 0) 1L << 40 else value.toLong
    }
    val rows = DistCollection.ofIntPairs(Array(1, 1, 2), Array(10, 20, 30), 2)
    val counted = rows.mapValues(widen).countByKey
    ColumnarPlanner.lower(counted.plan).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(IntLongMapValue(Scan)))",
    )
    calls.set(0)
    counted.collect().toSeq.sortBy(_._1) shouldBe Seq((1, 2), (2, 1))
    calls.get() shouldBe 0

    val nullable = nullKeys.mapValues(widen).countByKey
    calls.set(0)
    nullable.collect().toSeq.sortBy(_._1) shouldBe Seq((1, 2), (2, 2))
    calls.get() shouldBe 0

    val widened = rows.mapValues(widen)
    assert(ColumnarPlanner.lower(widened.filterValues((value: Long) => value > 0L).plan).isEmpty)
    assert(ColumnarPlanner.lower(widened.groupByKey.plan).isEmpty)
    assert(
      ColumnarPlanner
        .lower(widened.reduceByKey[Int, Long]((left: Long, _: Long) => left).plan)
        .isEmpty,
    )
  }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullKeys: DistCollection[(Int, Int)] =
    def cell(key: Any, value: Int): (Int, Int) =
      Tuple2(key, Int.box(value)).asInstanceOf[(Int, Int)]
    DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq(cell(1, 4), cell(null, 9), cell(1, 5))),
          Partition(Seq(cell(2, 6), cell(null, 8), cell(2, 7))),
        ),
      ),
    )
