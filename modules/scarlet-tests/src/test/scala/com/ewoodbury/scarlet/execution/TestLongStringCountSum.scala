package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Long and string count-sum against the row path.
 *
 * A null key is dropped. A null value is counted and not added. An empty string is a present key.
 * Zero stays a key and a value. Generic `groupByKey.map` does not lower to these nodes.
 */
class TestLongStringCountSum extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

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

  "countSumByLongKey" should "fold an owned scan, including zero and a negative key" in {
    val data = DistCollection.ofLongPairs(
      Array(1L, 0L, 1L, -2L, 0L),
      Array(4L, 1L, 5L, 3L, -1L),
      3,
    )
    ColumnarPlanner.lower(data.countSumByLongKey.plan).map(_.shape) shouldBe Some(
      "LongCountSum(Exchange(Scan))",
    )
    agrees(data.countSumByLongKey) shouldBe Map(
      1L -> (2, 9L),
      0L -> (2, 0L),
      -2L -> (1, 3L),
    )
  }

  it should "drop a null key and count a null value without adding it" in {
    val data = DistCollection(
      Seq(
        pair(longRef(1L), longRef(4L)),
        pair(longRef(1L), absentLong),
        pair(absentLong, longRef(9L)),
        pair(longRef(0L), longRef(0L)),
        pair(longRef(2L), absentLong),
        pair(longRef(2L), absentLong),
      ),
      3,
    )
    ColumnarPlanner.lower(data.countSumByLongKey.plan).map(_.shape) shouldBe Some(
      "LongCountSum(Exchange(Scan))",
    )
    agrees(data.countSumByLongKey) shouldBe Map(
      1L -> (2, 4L),
      0L -> (1, 0L),
      2L -> (2, 0L),
    )
  }

  "countSumByStringKey" should "keep an empty string and skip a null value" in {
    val data = stringSource()
    ColumnarPlanner.lower(data.countSumByStringKey.plan).map(_.shape) shouldBe Some(
      "StringCountSum(Exchange(Scan))",
    )
    agrees(data.countSumByStringKey) shouldBe Map(
      "a" -> (2, 7L),
      "" -> (1, 5L),
      "b" -> (2, 1L),
    )
  }

  "countSumByKey" should "still lower an owned int pair scan" in {
    val data = DistCollection.ofIntPairs(Array(1, 1, 2), Array(4, 5, 3), 2)
    ColumnarPlanner.lower(data.countSumByKey.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(Scan))",
    )
  }

  "groupByKey.map" should "not lower to a long count-sum" in {
    val data = DistCollection.ofLongPairs(Array(1L, 1L), Array(4L, 5L), 2)
    val mapped = data.groupByKey.map { row =>
      val folded = row._2.iterator.foldLeft((0, 0L)) { (acc, value) =>
        (acc._1 + 1, acc._2 + value)
      }
      (row._1, folded._1, folded._2)
    }
    ColumnarPlanner.lower(mapped.plan).map(_.shape) match
      case Some(shape) => shape.contains("LongCountSum") shouldBe false
      case None => succeed
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def stringSource(): DistCollection[(String, Int)] =
    DistCollection(
      Seq(
        stringPair("a", Integer.valueOf(3)),
        stringPair("a", Integer.valueOf(4)),
        stringPair("", Integer.valueOf(5)),
        stringPair(null, Integer.valueOf(9)),
        stringPair("b", absentInt),
        stringPair("b", Integer.valueOf(1)),
      ),
      3,
    )

  private def agrees[K](data: DistCollection[(K, Int, Long)]): Map[K, (Int, Long)] =
    val columnar = asMap[K](data.collect())
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      columnar shouldBe asMap[K](data.collect())
      columnar
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def asMap[K](rows: Iterable[(K, Int, Long)]): Map[K, (Int, Long)] =
    Map.from(rows.iterator.map(row => (row._1, (row._2, row._3))))

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
