package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Probe-time join sums against the row path.
 *
 * The oracle is a columnar-off `collect`, not a stored checksum. A null measured value that
 * matches adds as zero. A null that does not match stays columnar. Keys `0` and `80` share
 * a home slot in a 16-slot table and the same shuffle partition; their sums stay separate.
 */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
class TestJoinProbeSum extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val partitions = 2

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 8192,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
        spillEnabled = true,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    super.afterEach()

  "a join sum" should "match the row path for both columns" in {
    val left = pairs(Array(1, 1, 2), Array(10, 20, 3))
    val right = pairs(Array(1, 2, 2), Array(4, 5, 6))
    val joined = left.join(right)
    joined.sumJoinLeft.collect().toMap shouldBe Map(1 -> 30L, 2 -> 6L)
    joined.sumJoinLeft.collect().toMap shouldBe rowMap(joined.sumJoinLeft)
    joined.sumJoinRight.collect().toMap shouldBe Map(1 -> 8L, 2 -> 11L)
    joined.sumJoinRight.collect().toMap shouldBe rowMap(joined.sumJoinRight)
    ColumnarPlanner.tryRun(joined.sumJoinLeft.plan).isDefined shouldBe true
  }

  it should "keep two keys that share a hash slot" in {
    // tableSize(4) is 16. Both keys mix into slot 0 and both land in shuffle partition 0.
    val leftKey = 0
    val rightKey = 80
    (ColumnHash.mix(leftKey) & 15) shouldBe (ColumnHash.mix(rightKey) & 15)
    Math.floorMod(leftKey, 4) shouldBe Math.floorMod(rightKey, 4)
    ColumnHash.tableSize(4) shouldBe 16
    val left = pairs(Array(leftKey, leftKey, rightKey, rightKey), Array(5, 7, 50, 1))
    val right = pairs(Array(leftKey, rightKey, rightKey, rightKey), Array(4, 3, 3, 9))
    val joined = left.join(right)
    val leftSums = joined.sumJoinLeft.collect().toMap
    leftSums shouldBe rowMap(joined.sumJoinLeft)
    leftSums shouldBe Map(leftKey -> 12L, rightKey -> 153L)
    val rightSums = joined.sumJoinRight.collect().toMap
    rightSums shouldBe rowMap(joined.sumJoinRight)
    rightSums shouldBe Map(leftKey -> 8L, rightKey -> 30L)
  }

  it should "add zero for a null measured value only when that row matches" in {
    // A present pair is first so the scan witnesses int pairs. The null value is a later hole.
    val missed = nullPairs(Seq(cell(2, 5), cell(1, null)), Seq(cell(2, 1)))
    missed.sumJoinLeft.collect().toMap shouldBe Map(2 -> 5L)
    missed.sumJoinLeft.collect().toMap shouldBe rowMap(missed.sumJoinLeft)
    ColumnarPlanner.tryRun(missed.sumJoinLeft.plan).isDefined shouldBe true

    val matched = nullPairs(Seq(cell(2, 5), cell(1, null)), Seq(cell(1, 1), cell(2, 1)))
    matched.sumJoinLeft.collect().toMap shouldBe Map(1 -> 0L, 2 -> 5L)
    matched.sumJoinLeft.collect().toMap shouldBe rowMap(matched.sumJoinLeft)
    ColumnarPlanner.tryRun(matched.sumJoinLeft.plan).isDefined shouldBe true
  }

  it should "sum a match when the unmeasured side is null" in {
    val joined = nullPairs(Seq(cell(1, 10), cell(2, 4)), Seq(cell(2, 3), cell(1, null)))
    joined.sumJoinLeft.collect().toMap shouldBe Map(1 -> 10L, 2 -> 4L)
    joined.sumJoinLeft.collect().toMap shouldBe rowMap(joined.sumJoinLeft)
    ColumnarPlanner.tryRun(joined.sumJoinLeft.plan).isDefined shouldBe true
    joined.sumJoinRight.collect().toMap shouldBe Map(1 -> 0L, 2 -> 3L)
    joined.sumJoinRight.collect().toMap shouldBe rowMap(joined.sumJoinRight)
    ColumnarPlanner.tryRun(joined.sumJoinRight.plan).isDefined shouldBe true
  }

  it should "fall back when a build bucket exceeds the spill budget" in {
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(spillEnabled = true, spillBudgetBytes = 1L))
      ScarletRuntime.get.shuffle.clear()
      val joined = pairs(Array(1, 2), Array(10, 20)).join(pairs(Array(1, 2), Array(3, 4)))
      val summed = joined.sumJoinLeft
      ColumnarPlanner.lower(summed.plan).isDefined shouldBe true
      ColumnarPlanner.tryRun(summed.plan) shouldBe None
      summed.collect().toMap shouldBe rowMap(summed)
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
  }

  it should "publish an empty sum for an empty side" in {
    val present = pairs(Array(1, 2), Array(10, 20))
    val empty = pairs(Array.empty[Int], Array.empty[Int])
    val noLeft = empty.join(present).sumJoinLeft
    val noRight = present.join(empty).sumJoinLeft
    val neither = empty.join(empty).sumJoinRight
    noLeft.collect().toMap shouldBe Map.empty[Int, Long]
    noLeft.collect().toMap shouldBe rowMap(noLeft)
    noRight.collect().toMap shouldBe rowMap(noRight)
    neither.collect().toMap shouldBe rowMap(neither)
    ColumnarPlanner.tryRun(noLeft.plan).isDefined shouldBe true
    ColumnarPlanner.tryRun(noRight.plan).isDefined shouldBe true
    ColumnarPlanner.tryRun(neither.plan).isDefined shouldBe true
  }

  it should "sum a high-cardinality key set" in {
    val width = 2048
    val keys = Array.tabulate(width)(i => i + 1)
    val values = Array.tabulate(width)(i => i % 17)
    val probeKeys = Array.tabulate(width * 2)(i => (i % width) + 1)
    val probeValues = Array.fill(width * 2)(1)
    val summed = pairs(keys, values).join(pairs(probeKeys, probeValues)).sumJoinLeft
    summed.collect().toMap shouldBe rowMap(summed)
    ColumnarPlanner.tryRun(summed.plan).isDefined shouldBe true
  }

  it should "drop a null key and stay off broadcast and sort-merge" in {
    val joined = nullPairs(Seq(cell(1, 4), cell(null, 9)), Seq(cell(1, 2), cell(null, 8)))
    joined.sumJoinLeft.collect().toMap shouldBe Map(1 -> 4L)
    joined.sumJoinLeft.collect().toMap shouldBe rowMap(joined.sumJoinLeft)
    ColumnarPlanner.tryRun(joined.sumJoinLeft.plan).isDefined shouldBe true

    val left = pairs(Array(1, 2), Array(10, 20))
    val right = pairs(Array(1), Array(3))
    val sorted = left.sortMergeJoin(right).sumJoinLeft
    val broadcast = left.broadcastJoin(right).sumJoinLeft
    ColumnarPlanner.lower(sorted.plan).isEmpty shouldBe true
    ColumnarPlanner.lower(broadcast.plan).isEmpty shouldBe true
    sorted.collect().toMap shouldBe rowMap(sorted)
    broadcast.collect().toMap shouldBe rowMap(broadcast)

    val mapped = pairs(Array(11, 21), Array(4, 5)).mapIntKey(key => key % 10)
    val remapped = mapped.join(pairs(Array(1, 1), Array(3, 4))).sumJoinLeft
    ColumnarPlanner.lower(remapped.plan).isEmpty shouldBe true
    remapped.collect().toMap shouldBe rowMap(remapped)
  }

  "decodeSums" should "reject a hole and a short column" in {
    val present = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int64),
      Vector(
        Int32Column.of(Array(1, 2), Validity.allValid(2), 2),
        Int64Column.of(Array(10L, 20L), Validity.allValid(2), 2),
      ),
      2,
    )
    JoinProbeAggregate.decodeSums(present) shouldBe Vector((1, 10L), (2, 20L))

    val hole = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int64),
      Vector(
        Int32Column.of(Array(1, 0), Validity.pack(2, row => row == 0), 2),
        Int64Column.of(Array(10L, 0L), Validity.allValid(2), 2),
      ),
      2,
    )
    an[IllegalArgumentException] should be thrownBy JoinProbeAggregate.decodeSums(hole)

    val valueHole = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int64),
      Vector(
        Int32Column.of(Array(1, 2), Validity.allValid(2), 2),
        Int64Column.of(Array(10L, 0L), Validity.pack(2, row => row == 0), 2),
      ),
      2,
    )
    an[IllegalArgumentException] should be thrownBy JoinProbeAggregate.decodeSums(valueHole)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def cell(key: Any, value: Any): (Any, Any) = (key, value)

  private def pairs(keys: Array[Int], values: Array[Int]): DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(keys, values, partitions)

  private def rowMap(data: DistCollection[(Int, Long)]): Map[Int, Long] =
    row(data.collect().toMap)

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullPairs(
      left: Seq[(Any, Any)],
      right: Seq[(Any, Any)],
  ): DistCollection[(Int, (Int, Int))] =
    def source(rows: Seq[(Any, Any)]): DistCollection[(Int, Int)] =
      DistCollection(Plan.Source(Seq(Partition(rows))).asInstanceOf[Plan[(Int, Int)]])
    source(left).join(source(right))
