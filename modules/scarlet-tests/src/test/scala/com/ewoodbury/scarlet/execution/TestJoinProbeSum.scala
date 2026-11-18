package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

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
 * matches adds as zero. A null that does not match stays columnar. Keys `0` and `80` share a home
 * slot in a 16-slot table and the same shuffle partition; their sums stay separate.
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
    ColumnarPlanner.lower(joined.sumJoinLeft.plan).map(_.shape) shouldBe Some(
      "JoinProbeSumLeft(Exchange(Scan), Exchange(Scan))",
    )
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

    val sorted = left.sortMergeJoin(right).sumJoinLeft
    sorted.collect().toMap shouldBe leftSums
    sorted.collect().toMap shouldBe rowMap(sorted)
    val sortedArm = ColumnarPlanner.lower(sorted.plan).map(_.shape).getOrElse("")
    sortedArm.contains("JoinProbeSum") shouldBe true
    sortedArm.contains("SortMergeJoin") shouldBe false

    val broadcast = left.broadcastJoin(right).sumJoinLeft
    broadcast.collect().toMap shouldBe leftSums
    broadcast.collect().toMap shouldBe rowMap(broadcast)
    val broadcastArm = ColumnarPlanner.lower(broadcast.plan).map(_.shape).getOrElse("")
    broadcastArm.contains("JoinProbeSum") shouldBe true
    broadcastArm.contains("Exchange") shouldBe false
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

    val broadcastMatched = nullPairs(
      Seq(cell(2, 5), cell(1, null)),
      Seq(cell(1, 1), cell(2, 1)),
      Some(Plan.JoinStrategy.Broadcast),
    ).sumJoinLeft
    broadcastMatched.collect().toMap shouldBe Map(1 -> 0L, 2 -> 5L)
    broadcastMatched.collect().toMap shouldBe rowMap(broadcastMatched)
    ColumnarPlanner.tryRun(broadcastMatched.plan).isDefined shouldBe true
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
      val sorted = pairs(Array(1, 2), Array(10, 20))
        .sortMergeJoin(pairs(Array(1, 2), Array(3, 4)))
        .sumJoinLeft
      ColumnarPlanner.lower(sorted.plan).isDefined shouldBe true
      ColumnarPlanner.tryRun(sorted.plan).isEmpty shouldBe true
      sorted.collect().toMap shouldBe rowMap(sorted)
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
    val sortedEmpty = empty.sortMergeJoin(present).sumJoinLeft
    sortedEmpty.collect().toMap shouldBe rowMap(sortedEmpty)
    ColumnarPlanner.tryRun(sortedEmpty.plan).isDefined shouldBe true
    val broadcastEmpty = empty.broadcastJoin(present).sumJoinLeft
    broadcastEmpty.collect().toMap shouldBe rowMap(broadcastEmpty)
    ColumnarPlanner.tryRun(broadcastEmpty.plan).isDefined shouldBe true
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
    val broadcast = pairs(keys, values)
      .broadcastJoin(pairs(probeKeys, probeValues))
      .sumJoinLeft
    broadcast.collect().toMap shouldBe rowMap(broadcast)
    ColumnarPlanner.tryRun(broadcast.plan).isDefined shouldBe true
  }

  it should "drop a null key, hash an explicit sort-merge sum, and probe a broadcast sum" in {
    val joined = nullPairs(Seq(cell(1, 4), cell(null, 9)), Seq(cell(1, 2), cell(null, 8)))
    joined.sumJoinLeft.collect().toMap shouldBe Map(1 -> 4L)
    joined.sumJoinLeft.collect().toMap shouldBe rowMap(joined.sumJoinLeft)
    ColumnarPlanner.tryRun(joined.sumJoinLeft.plan).isDefined shouldBe true

    val left = pairs(Array(1, 2), Array(10, 20))
    val right = pairs(Array(1), Array(3))
    val sorted = left.sortMergeJoin(right).sumJoinLeft
    val sortedRight = left.sortMergeJoin(right).sumJoinRight
    val broadcast = left.broadcastJoin(right).sumJoinLeft
    val broadcastRight = left.broadcastJoin(right).sumJoinRight
    val arm = ColumnarPlanner.lower(sorted.plan).map(_.shape).getOrElse("")
    arm shouldBe "JoinProbeSumLeft(Exchange(Scan), Exchange(Scan))"
    arm.contains("JoinProbeSum") shouldBe true
    arm.contains("SortMergeJoin") shouldBe false
    ColumnarPlanner.tryRun(sorted.plan).isDefined shouldBe true
    sorted.collect().toMap shouldBe Map(1 -> 10L)
    sorted.collect().toMap shouldBe rowMap(sorted)
    val rightArm = ColumnarPlanner.lower(sortedRight.plan).map(_.shape).getOrElse("")
    rightArm shouldBe "JoinProbeSumRight(Exchange(Scan), Exchange(Scan))"
    rightArm.contains("SortMergeJoin") shouldBe false
    sortedRight.collect().toMap shouldBe Map(1 -> 3L)
    sortedRight.collect().toMap shouldBe rowMap(sortedRight)
    val broadcastArm = ColumnarPlanner.lower(broadcast.plan).map(_.shape).getOrElse("")
    broadcastArm shouldBe "JoinProbeSumLeft(Scan, Scan)"
    broadcastArm.contains("JoinProbeSum") shouldBe true
    broadcastArm.contains("Exchange") shouldBe false
    ColumnarPlanner.tryRun(broadcast.plan).isDefined shouldBe true
    broadcast.collect().toMap shouldBe Map(1 -> 10L)
    broadcast.collect().toMap shouldBe rowMap(broadcast)
    val broadcastRightArm = ColumnarPlanner.lower(broadcastRight.plan).map(_.shape).getOrElse("")
    broadcastRightArm shouldBe "JoinProbeSumRight(Scan, Scan)"
    broadcastRightArm.contains("Exchange") shouldBe false
    ColumnarPlanner.tryRun(broadcastRight.plan).isDefined shouldBe true
    broadcastRight.collect().toMap shouldBe Map(1 -> 3L)
    broadcastRight.collect().toMap shouldBe rowMap(broadcastRight)

    val broadcastNull = nullPairs(
      Seq(cell(1, 4), cell(null, 9)),
      Seq(cell(1, 2), cell(null, 8)),
      Some(Plan.JoinStrategy.Broadcast),
    ).sumJoinLeft
    broadcastNull.collect().toMap shouldBe Map(1 -> 4L)
    broadcastNull.collect().toMap shouldBe rowMap(broadcastNull)
    ColumnarPlanner.tryRun(broadcastNull.plan).isDefined shouldBe true

    val sortedNull = nullPairs(
      Seq(cell(1, 4), cell(null, 9)),
      Seq(cell(1, 2), cell(null, 8)),
      Some(Plan.JoinStrategy.SortMerge),
    ).sumJoinLeft
    sortedNull.collect().toMap shouldBe Map(1 -> 4L)
    sortedNull.collect().toMap shouldBe rowMap(sortedNull)
    ColumnarPlanner.tryRun(sortedNull.plan).isDefined shouldBe true

    val sortedNullValue = nullPairs(
      Seq(cell(2, 5), cell(1, null)),
      Seq(cell(1, 1), cell(2, 1)),
      Some(Plan.JoinStrategy.SortMerge),
    ).sumJoinLeft
    sortedNullValue.collect().toMap shouldBe Map(1 -> 0L, 2 -> 5L)
    sortedNullValue.collect().toMap shouldBe rowMap(sortedNullValue)

    val filtered = pairs(Array(1, 2, 3, 4), Array(10, -1, 7, 8))
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .sortMergeJoin(
        pairs(Array(1, 2, 4, 5), Array(3, 60, 1, 9))
          .filterKeys((key: Int) => key % 3 != 0)
          .filterValues((value: Int) => value < 50),
      )
      .sumJoinLeft
    ColumnarPlanner.lower(filtered.plan).map(_.shape) shouldBe Some(
      "JoinProbeSumLeft(Exchange(FilterValue(FilterKey(Scan))), " +
        "Exchange(FilterValue(FilterKey(Scan))))",
    )
    filtered.collect().toMap shouldBe rowMap(filtered)

    val broadcastFiltered = pairs(Array(1, 2, 3, 4), Array(10, -1, 7, 8))
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .broadcastJoin(
        pairs(Array(1, 2, 4, 5), Array(3, 60, 1, 9))
          .filterKeys((key: Int) => key % 3 != 0)
          .filterValues((value: Int) => value < 50),
      )
      .sumJoinLeft
    ColumnarPlanner.lower(broadcastFiltered.plan).map(_.shape) shouldBe Some(
      "JoinProbeSumLeft(FilterValue(FilterKey(Scan)), FilterValue(FilterKey(Scan)))",
    )
    broadcastFiltered.collect().toMap shouldBe rowMap(broadcastFiltered)
  }

  it should "fall back when a broadcast build exceeds the broadcast budget" in {
    val saved = ScarletConf.get
    try
      val left = pairs(Array(1, 2), Array(10, 20))
      val right = pairs(Array(1), Array(3))
      val summed = left.broadcastJoin(right).sumJoinLeft
      val estimate = HashJoin.retainedBuildBytes(2)
      ScarletConf.set(saved.copy(broadcastBuildBudgetBytes = estimate - 1L))
      ScarletRuntime.get.shuffle.clear()
      ColumnarPlanner.lower(summed.plan).map(_.shape) shouldBe Some(
        "JoinProbeSumLeft(Scan, Scan)",
      )
      ColumnarPlanner.tryRun(summed.plan) shouldBe None
      summed.collect().toMap shouldBe rowMap(summed)
      val emptyLeft = pairs(Array.empty[Int], Array.empty[Int]).broadcastJoin(right).sumJoinLeft
      ColumnarPlanner.tryRun(emptyLeft.plan).isDefined shouldBe true
      emptyLeft.collect().toMap shouldBe rowMap(emptyLeft)
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
  }

  it should "sum a remapped key from the probe without writing matches" in {
    val mapped = pairs(Array(11, 21), Array(4, 5)).mapIntKey(key => key % 10)
    val right = pairs(Array(1, 1), Array(3, 4))
    val remapped = mapped.join(right).sumJoinLeft
    ColumnarPlanner.lower(remapped.plan).map(_.shape) shouldBe Some(
      "JoinProbeSumLeft(Exchange(MapKey(Scan)), Exchange(Scan))",
    )
    ColumnarPlanner.tryRun(remapped.plan).isDefined shouldBe true
    remapped.collect().toMap shouldBe Map(1 -> 18L)
    remapped.collect().toMap shouldBe rowMap(remapped)

    val broadcastMapped = mapped.broadcastJoin(right).sumJoinLeft
    ColumnarPlanner.lower(broadcastMapped.plan).map(_.shape) shouldBe Some(
      "JoinProbeSumLeft(MapKey(Scan), Scan)",
    )
    ColumnarPlanner.tryRun(broadcastMapped.plan).isDefined shouldBe true
    broadcastMapped.collect().toMap shouldBe Map(1 -> 18L)
    broadcastMapped.collect().toMap shouldBe rowMap(broadcastMapped)

    val remappedRight = mapped.join(right).sumJoinRight
    ColumnarPlanner.lower(remappedRight.plan).map(_.shape) shouldBe Some(
      "JoinProbeSumRight(Exchange(MapKey(Scan)), Exchange(Scan))",
    )
    ColumnarPlanner.tryRun(remappedRight.plan).isDefined shouldBe true
    remappedRight.collect().toMap shouldBe Map(1 -> 14L)
    remappedRight.collect().toMap shouldBe rowMap(remappedRight)

    val probeMapped = pairs(Array(1, 2), Array(10, 20))
      .join(pairs(Array(11, 22), Array(3, 4)).mapIntKey(key => key % 10))
      .sumJoinLeft
    ColumnarPlanner.lower(probeMapped.plan).map(_.shape) shouldBe Some(
      "JoinProbeSumLeft(Exchange(Scan), Exchange(MapKey(Scan)))",
    )
    probeMapped.collect().toMap shouldBe Map(1 -> 10L, 2 -> 20L)
    probeMapped.collect().toMap shouldBe rowMap(probeMapped)

    val above = pairs(Array(1, 2), Array(3, 4)).mapIntKey(key => key + 1)
    val blocked = above.filterKeys((key: Int) => key > 0).join(right).sumJoinLeft
    ColumnarPlanner.lower(blocked.plan).isEmpty shouldBe true

    // Filters stay under the map. Skipping the map would leave keys 11 and 13, which miss.
    val filtered = pairs(Array(11, 12, 13), Array(10, -1, 7))
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .mapIntKey(key => key % 10)
      .join(pairs(Array(1, 2), Array(3, 4)))
      .sumJoinLeft
    ColumnarPlanner.lower(filtered.plan).map(_.shape) shouldBe Some(
      "JoinProbeSumLeft(Exchange(MapKey(FilterValue(FilterKey(Scan)))), Exchange(Scan))",
    )
    filtered.collect().toMap shouldBe Map(1 -> 10L)
    filtered.collect().toMap shouldBe rowMap(filtered)

    val twice = pairs(Array(11, 21), Array(4, 5)).mapIntKey(_ + 1).mapIntKey(_ % 10)
    val nested = twice.join(pairs(Array(2, 2), Array(3, 4))).sumJoinLeft
    ColumnarPlanner.lower(nested.plan).map(_.shape) shouldBe Some(
      "JoinProbeSumLeft(Exchange(MapKey(MapKey(Scan))), Exchange(Scan))",
    )
    nested.collect().toMap shouldBe Map(2 -> 18L)
    nested.collect().toMap shouldBe rowMap(nested)
  }

  it should "drop a null key from mapIntKey and not call the function on it" in {
    val calls = new AtomicInteger
    val summed = boxedPairs(Seq(cell(1, 4), cell(null, 9), cell(2, 5)))
      .mapIntKey { key =>
        calls.incrementAndGet()
        key + 1
      }
      .join(boxedPairs(Seq(cell(2, 7), cell(null, 8), cell(3, 1))))
      .sumJoinLeft
    calls.set(0)
    val columnar = summed.collect().toMap
    calls.get() shouldBe 2
    calls.set(0)
    val rows = rowMap(summed)
    calls.get() shouldBe 2
    columnar shouldBe rows
    columnar shouldBe Map(2 -> 4L, 3 -> 5L)
    ColumnarPlanner.lower(summed.plan).map(_.shape) shouldBe Some(
      "JoinProbeSumLeft(Exchange(MapKey(Scan)), Exchange(Scan))",
    )
    ColumnarPlanner.tryRun(summed.plan).isDefined shouldBe true

    val broadcast = boxedPairs(Seq(cell(1, 4), cell(null, 9), cell(2, 5)))
      .mapIntKey { key =>
        calls.incrementAndGet()
        key + 1
      }
      .broadcastJoin(boxedPairs(Seq(cell(2, 7), cell(null, 8), cell(3, 1))))
      .sumJoinLeft
    calls.set(0)
    val broadcastSums = broadcast.collect().toMap
    calls.get() shouldBe 2
    calls.set(0)
    broadcastSums shouldBe rowMap(broadcast)
    calls.get() shouldBe 2
    broadcastSums shouldBe Map(2 -> 4L, 3 -> 5L)
    val broadcastShape = ColumnarPlanner.lower(broadcast.plan).map(_.shape).getOrElse("")
    broadcastShape shouldBe "JoinProbeSumLeft(MapKey(Scan), Scan)"
    broadcastShape.contains("Exchange") shouldBe false
    ColumnarPlanner.tryRun(broadcast.plan).isDefined shouldBe true
  }

  it should "match batch size 1 to the 8192 filtered sum" in {
    val left = pairs(Array(1, 2, 3, 4, 5, 8), Array(10, -1, 7, 8, 0, 4))
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val right = pairs(Array(1, 2, 4, 5, 8), Array(3, 60, 1, 9, 2))
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value < 50)
    val summed = left.join(right).sumJoinLeft
    val wide = summed.collect().toMap
    wide shouldBe rowMap(summed)
    val rightWide = left.join(right).sumJoinRight.collect().toMap
    rightWide shouldBe rowMap(left.join(right).sumJoinRight)
    val plain = pairs(Array(0, 80, 1), Array(5, 7, 9))
      .join(pairs(Array(0, 80), Array(4, 3)))
      .sumJoinLeft
    val plainWide = plain.collect().toMap
    plainWide shouldBe Map(0 -> 5L, 80 -> 7L)
    plainWide shouldBe rowMap(plain)
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarBatchSize = 1))
      ScarletRuntime.get.shuffle.clear()
      summed.collect().toMap shouldBe wide
      left.join(right).sumJoinRight.collect().toMap shouldBe rightWide
      plain.collect().toMap shouldBe plainWide
      ScarletConf.set(saved.copy(defaultShufflePartitions = 1))
      ScarletRuntime.get.shuffle.clear()
      summed.collect().toMap shouldBe wide
      plain.collect().toMap shouldBe plainWide
      val single = DistCollection
        .ofIntPairs(Array(1, 1, 2), Array(10, 20, 3), 1)
        .join(DistCollection.ofIntPairs(Array(1, 2), Array(4, 5), 1))
        .sumJoinLeft
      single.collect().toMap shouldBe rowMap(single)
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
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

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def boxedPairs(rows: Seq[(Any, Any)]): DistCollection[(Int, Int)] =
    DistCollection(Plan.Source(Seq(Partition(rows))).asInstanceOf[Plan[(Int, Int)]])

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
      strategy: Option[Plan.JoinStrategy] = None,
  ): DistCollection[(Int, (Int, Int))] =
    def source(rows: Seq[(Any, Any)]): DistCollection[(Int, Int)] =
      DistCollection(Plan.Source(Seq(Partition(rows))).asInstanceOf[Plan[(Int, Int)]])
    DistCollection(Plan.JoinOp(source(left).plan, source(right).plan, strategy))
