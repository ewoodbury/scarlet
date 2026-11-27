package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Long and string value sorts against the row path.
 *
 * Order is value descending, then key ascending. A null long orders as zero and stays null. A null
 * string key orders as `""` and stays null. A null int value orders as zero and stays null. Ties
 * are avoided so the sampled row partitioner and the rank split must agree.
 */
class TestLongStringValueSort extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

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
        spillEnabled = true,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "sortByLongValueDesc" should "order an owned scan by value, then key" in {
    val data = DistCollection.ofLongPairs(
      Array(5L, 1L, 3L, 8L, 2L, 0L, 7L, 4L, 6L, 9L),
      Array(1L, 9L, 9L, 0L, 4L, 4L, -1L, 9L, 2L, 1L),
      3,
    )
    shape(data.sortByLongValueDesc) shouldBe "SortByLongValueDesc(Scan)"
    agrees(data.sortByLongValueDesc) shouldBe Seq(
      1L -> 9L,
      3L -> 9L,
      4L -> 9L,
      0L -> 4L,
      2L -> 4L,
      6L -> 2L,
      5L -> 1L,
      9L -> 1L,
      8L -> 0L,
      7L -> -1L,
    )
  }

  it should "keep a null key and a null value, ordering each as zero" in {
    val data = longNulls()
    shape(data.sortByLongValueDesc) shouldBe "SortByLongValueDesc(Scan)"
    agrees(data.sortByLongValueDesc) shouldBe Seq(
      pair(longRef(1L), longRef(10L)),
      pair(absentLong, longRef(7L)),
      pair(longRef(3L), longRef(5L)),
      pair(longRef(4L), longRef(1L)),
      pair(longRef(2L), absentLong),
    )
  }

  it should "lower one project, one mapValues, and one slot, and refuse the rest" in {
    val data = DistCollection.ofLongPairs(Array(1L, 2L, 3L), Array(3L, 1L, 2L), 2)
    val projected = data.map((pair: (Long, Long)) => (pair._1, pair._2))
    val mapped = data.mapValues((value: Long) => value + 1L)
    shape(projected.sortByLongValueDesc) shouldBe "SortByLongValueDesc(Project(Scan))"
    shape(mapped.sortByLongValueDesc) shouldBe "SortByLongValueDesc(MapValue(Scan))"
    shape(projected.filterKeys((key: Long) => key > 0L).sortByLongValueDesc) shouldBe
      "SortByLongValueDesc(FilterKey(Project(Scan)))"
    shape(data.filterValues((value: Long) => value > 0L).sortByLongValueDesc) shouldBe
      "SortByLongValueDesc(FilterValue(Scan))"
    ColumnarPlanner.lower(
      data
        .mapValues((value: Long) => value + 1L)
        .mapValues((value: Long) => value + 1L)
        .sortByLongValueDesc
        .plan,
    ) shouldBe None
    ColumnarPlanner.lower(
      mapped.filterValues((value: Long) => value > 0L).sortByLongValueDesc.plan,
    ) shouldBe None
    ColumnarPlanner.lower(
      projected.filter((pair: (Long, Long)) => pair._2 > 0L).sortByLongValueDesc.plan,
    ) shouldBe None
    agrees(mapped.sortByLongValueDesc) shouldBe Seq(1L -> 4L, 3L -> 3L, 2L -> 2L)
    ColumnarPlanner.tryTake(data.sortByLongValueDesc.plan, 1).map(_.size) shouldBe Some(1)
  }

  "sortByStringValueDesc" should "order by the int value, then the string key" in {
    val data = DistCollection(Seq("b" -> 1, "a" -> 3, "c" -> 3, "" -> 2, "d" -> 0, "aa" -> 3), 3)
    shape(data.sortByStringValueDesc) shouldBe "SortByStringValueDesc(Scan)"
    agrees(data.sortByStringValueDesc) shouldBe Seq(
      "a" -> 3,
      "aa" -> 3,
      "c" -> 3,
      "" -> 2,
      "b" -> 1,
      "d" -> 0,
    )
  }

  it should "keep a null key and a null value" in {
    val data = stringNulls()
    shape(data.sortByStringValueDesc) shouldBe "SortByStringValueDesc(Scan)"
    agrees(data.sortByStringValueDesc) shouldBe Seq(
      stringPair("b", intRef(4)),
      stringPair(null, intRef(2)),
      stringPair("a", intRef(2)),
      stringPair("c", absentInt),
    )
  }

  it should "lower one project and one mapValues, and refuse a second map" in {
    val data = DistCollection(Seq("b" -> 1, "a" -> 3), 2)
    val projected = data.map((pair: (String, Int)) => (pair._1, pair._2))
    val mapped = data.mapValues((value: Int) => value + 1)
    shape(projected.sortByStringValueDesc) shouldBe "SortByStringValueDesc(Project(Scan))"
    shape(mapped.sortByStringValueDesc) shouldBe "SortByStringValueDesc(MapValue(Scan))"
    ColumnarPlanner.lower(
      data
        .map((pair: (String, Int)) => (pair._1, pair._2))
        .map((pair: (String, Int)) => (pair._1, pair._2))
        .sortByStringValueDesc
        .plan,
    ) shouldBe None
    agrees(mapped.sortByStringValueDesc) shouldBe Seq("a" -> 4, "b" -> 2)
  }

  "sortByIntValueDesc" should "still lower an int pair scan" in {
    val data = DistCollection.ofIntPairs(Array(1, 2), Array(2, 1), 2)
    shape(data.sortByIntValueDesc) shouldBe "SortByIntValueDesc(Scan)"
  }

  private def shape[A](data: DistCollection[A]): String =
    ColumnarPlanner.lower(data.plan).map(_.shape).getOrElse("none")

  private def agrees[A](data: DistCollection[A]): Seq[A] =
    val columnar = data.collect().toSeq
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      val rows = data.collect().toSeq
      columnar shouldBe rows
      columnar
    finally ScarletConf.set(saved)

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def longNulls(): DistCollection[(Long, Long)] =
    DistCollection(
      Seq(
        pair(longRef(1L), longRef(10L)),
        pair(absentLong, longRef(7L)),
        pair(longRef(3L), longRef(5L)),
        pair(longRef(4L), longRef(1L)),
        pair(longRef(2L), absentLong),
      ),
      3,
    )

  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.AsInstanceOf"))
  private def stringNulls(): DistCollection[(String, Int)] =
    DistCollection(
      Seq(
        stringPair("b", intRef(4)),
        stringPair(null, intRef(2)),
        stringPair("a", intRef(2)),
        stringPair("c", absentInt),
      ),
      3,
    )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def pair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentLong: java.lang.Long = null

  private def longRef(value: Long): java.lang.Long = java.lang.Long.valueOf(value)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def stringPair(key: String, value: java.lang.Integer): (String, Int) =
    Tuple2(key, value).asInstanceOf[(String, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentInt: java.lang.Integer = null

  private def intRef(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)
