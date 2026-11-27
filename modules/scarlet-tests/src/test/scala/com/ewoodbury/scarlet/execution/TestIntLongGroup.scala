package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * `groupByKey` after `mapValues` to long builds the lists on columns.
 *
 * The fake leaves `lower` empty. A null key is dropped. A null value stays in the iterable. A
 * filter of the widening stays a value filter. A non-adding `reduceByKey` of the bare widening is
 * an int-long hash aggregate. Replacing the group with a combine would drop the lists.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Null",
  ),
)
class TestIntLongGroup extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  it should "group long values by an int key" in {
    val calls = new AtomicInteger
    val widen: Int => Long = value => {
      val _ = calls.incrementAndGet()
      if (value == 0) 1L << 40 else value.toLong
    }
    val rows = DistCollection.ofIntPairs(Array(1, 1, 2), Array(0, 20, 30), 2)
    val grouped = rows.mapValues(widen).groupByKey
    val lowered = ColumnarPlanner.lower(grouped.plan)
    lowered.map(_.shape) shouldBe Some("GroupByKey(Exchange(IntLongMapValue(Scan)))")
    lowered match
      case Some(PhysicalOp.GroupByKey(PhysicalOp.Exchange(_: PhysicalOp.IntLongMapValue))) =>
        succeed
      case other => fail(s"expected the int-long group, got $other")

    calls.set(0)
    ColumnarPlanner.tryRun(grouped.plan).isDefined shouldBe true
    calls.set(0)
    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      grouped.collect()
    }
    sink.batchCount should be > 0L
    calls.get() shouldBe 3
    show(found) shouldBe show(row(grouped.collect()))
    calls.set(0)
    grouped.count() shouldBe 2L
    calls.get() shouldBe 0

    val nullable = nullKeys.mapValues(widen).groupByKey
    calls.set(0)
    val dropped = nullable.collect()
    calls.get() shouldBe 6
    show(dropped) shouldBe show(row(nullable.collect()))

    // A present pair is first, so the scan is classified as int pairs. A leading null component
    // does not witness pairs, and that plan stays on rows. Row `mapValues` then unboxes the null
    // `Int` to `0`, so this collect is not compared to that action. The columnar list keeps the
    // null and does not call the function on it. `0L` would be a present value.
    val keptNull = nullValue.mapValues(widen).groupByKey
    ColumnarPlanner.lower(keptNull.plan).map(_.shape) shouldBe Some(
      "GroupByKey(Exchange(IntLongMapValue(Scan)))",
    )
    calls.set(0)
    val kept = keptNull.collect()
    calls.get() shouldBe 1
    show(kept) shouldBe Seq((1, List("4", "null")))

    val widened = rows.mapValues(widen)
    ColumnarPlanner
      .lower(widened.filterValues((value: Long) => value > 0L).plan)
      .map(_.shape) shouldBe
      Some("FilterValue(IntLongMapValue(Scan))")
    ColumnarPlanner
      .lower(widened.reduceByKey[Int, Long]((left: Long, _: Long) => left).plan)
      .map(_.shape) shouldBe Some("IntLongHashAggregate(Exchange(IntLongMapValue(Scan)))")
  }

  /**
   * Key order is sorted. A null long is the text `null`, so a truncated int cannot match. The
   * iterable is read as references so a null `Long` is not unboxed.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def show(rows: Iterable[(Int, Iterable[Long])]): Seq[(Int, List[String])] =
    rows.iterator
      .map { (key, values) =>
        val raw = values.asInstanceOf[Iterable[AnyRef]]
        val texts = raw.iterator.map { cell =>
          if (cell == null) "null" else cell.toString
        }.toList
        (key, texts)
      }
      .toSeq
      .sortBy(_._1)

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

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullValue: DistCollection[(Int, Int)] =
    def cell(key: Int, value: Any): (Int, Int) =
      Tuple2(Int.box(key), value).asInstanceOf[(Int, Int)]
    DistCollection(
      Plan.Source(
        Seq(Partition(Seq(cell(1, Int.box(4)), cell(1, null)))),
      ),
    )

  /** Row collect. Restores the previous conf. Columnar execution does not stay off. */
  private def row[A](body: => A): A =
    val previous = ScarletConf.get
    try
      ScarletConf.set(previous.copy(columnarExecution = false, threadPoolSize = 1))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(previous)
      ScarletRuntime.get.shuffle.clear()
