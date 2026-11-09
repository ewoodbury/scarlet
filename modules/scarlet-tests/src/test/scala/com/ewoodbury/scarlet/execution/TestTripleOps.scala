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
 * Columnar whole-row `filter` and same-type `map` on an int join triple.
 *
 * The row path calls the function on every row. Reading a null `Int` unboxes it to 0, so a
 * predicate that accepts 0 keeps the row. The columnar path follows the int-pair row filter: a
 * null component drops the row and the predicate is not called. A map is not called on that row
 * either. The stored values and validity bits are copied, so absence stays absence. A present 0
 * is a real value. Those null rules are checked on their own. Present rows, including a present
 * 0, match the row path.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TestTripleOps extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "filter" should "match the row path for present rows, including a present zero" in {
    val joined = presentJoin
    val filtered = joined.filter((row: (Int, (Int, Int))) => row._2._1 > 0 && row._2._2 >= 0)

    shows(columnar(filtered)) shouldBe shows(row(filtered.collect().toList))
    ColumnarPlanner.lower(filtered.plan).map(_.shape) shouldBe Some(
      "FilterTripleRow(HashJoin(Exchange(Scan), Exchange(Scan)))",
    )
  }

  it should "call the predicate once per present row and not on a null component" in {
    val calls = new AtomicInteger
    val filtered = nullableJoin.filter { (row: (Int, (Int, Int))) =>
      calls.incrementAndGet()
      row._2._1 > 0
    }
    val got = shows(columnar(filtered))

    // Keys 1 and 5 are present and kept. Key 3 is a present zero and rejected. Keys 2 and 4 have a
    // null component and are not offered to the predicate. The row path still calls the predicate
    // on key 4: it never reads the null right, so left 8 keeps that row.
    calls.get() shouldBe 3
    got shouldBe Seq("1,10,100", "5,4,0")
    shows(row(nullableJoin.filter((row: (Int, (Int, Int))) => row._2._1 > 0).collect().toList)) shouldBe Seq(
      "1,10,100",
      "4,8,null",
      "5,4,0",
    )
  }

  it should "drop a null component without calling the predicate, even when the predicate accepts zero" in {
    val calls = new AtomicInteger
    val filtered = nullableJoin.filter { (_: (Int, (Int, Int))) =>
      calls.incrementAndGet()
      true
    }

    val got = shows(columnar(filtered))
    calls.get() shouldBe 3
    got shouldBe Seq("1,10,100", "3,0,7", "5,4,0")
    row(nullableJoin.filter((_: (Int, (Int, Int))) => true).collect().toList).length shouldBe 5
  }

  it should "keep a present zero that the predicate accepts" in {
    val calls = new AtomicInteger
    val filtered = nullableJoin.filter { (row: (Int, (Int, Int))) =>
      calls.incrementAndGet()
      row._2._2 == 0
    }

    val got = shows(columnar(filtered))
    // Key 5 is a present 0 and is kept. Key 4's null right is not offered to the predicate. The
    // row path unboxes that null to 0, so `== 0` keeps key 4 as well.
    calls.get() shouldBe 3
    got shouldBe Seq("5,4,0")
    shows(row(filtered.collect().toList)) shouldBe Seq("4,8,null", "5,4,0")
  }

  "map" should "match the row path for present rows and call the function once per row" in {
    val calls = new AtomicInteger
    val mapped = presentJoin.map { (row: (Int, (Int, Int))) =>
      calls.incrementAndGet()
      (row._1, (row._2._1 + 1, row._2._2 + 1))
    }

    val got = shows(columnar(mapped))
    calls.get() shouldBe 8
    got shouldBe shows(row(mapped.collect().toList))
    ColumnarPlanner.lower(mapped.plan).map(_.shape) shouldBe Some(
      "Project(HashJoin(Exchange(Scan), Exchange(Scan)))",
    )
  }

  it should "skip a null component, preserve that absence, and still map a present zero" in {
    val calls = new AtomicInteger
    val mapped = Plan.MapOp(
      nullableJoin.plan,
      (row: (Int, (Int, Int))) => {
        calls.incrementAndGet()
        if (row._2._1 == 0) then nullLeft(row._1, row._2._2)
        else (row._1, (row._2._1 + 1, row._2._2 + 1))
      },
    )

    shows(columnar(DistCollection(mapped))) shouldBe Seq(
      "1,11,101",
      "2,null,20",
      "3,null,7",
      "4,8,null",
      "5,5,1",
    )
    calls.get() shouldBe 3
  }

  it should "store a mapped zero as present" in {
    val mapped = nullableJoin.map((row: (Int, (Int, Int))) => (row._1, (0, row._2._2)))
    val got = shows(columnar(mapped))

    got should contain("1,0,100")
    got should contain("5,0,0")
    got.exists(_.contains("null")) shouldBe true
  }

  "empty partitions" should "publish no row and call neither the predicate nor the map" in {
    val left = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 2)
    val right = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 2)
    val joined = left.join(right)
    val filterCalls = new AtomicInteger
    val mapCalls = new AtomicInteger
    val filtered = joined.filter { (row: (Int, (Int, Int))) =>
      filterCalls.incrementAndGet()
      row._2._1 > 0
    }
    val mapped = joined.map { (row: (Int, (Int, Int))) =>
      mapCalls.incrementAndGet()
      (row._1, (row._2._1, row._2._2))
    }
    withConf(batchSize = 2, threads = 1, partitions = 1)
    val filterSink = new CountingSink
    val mapSink = new CountingSink
    val filterRows = ColumnMetrics.withSink(filterSink)(columnar(filtered))
    val mapRows = ColumnMetrics.withSink(mapSink)(columnar(mapped))

    filterRows shouldBe Seq.empty[(Int, (Int, Int))]
    mapRows shouldBe Seq.empty[(Int, (Int, Int))]
    filterCalls.get() shouldBe 0
    mapCalls.get() shouldBe 0
    filterSink.visitCount should be > 0L
    mapSink.visitCount should be > 0L
    filterRows shouldBe row(filtered.collect().toList)
  }

  "morsels" should "match the row path when a batch is split, on one thread and on several" in {
    val joined = presentJoin
    val filtered = joined.filter((row: (Int, (Int, Int))) => row._2._2 > 200)
    val mapped = joined.map((row: (Int, (Int, Int))) => (row._1, (row._2._1 + 1, row._2._2 + 1)))
    val expectedFilter = shows(row(filtered.collect().toList))
    val expectedMap = shows(row(mapped.collect().toList))

    withConf(batchSize = 2, threads = 1, partitions = 1)
    val sink = new CountingSink
    val got = ColumnMetrics.withSink(sink)(shows(columnar(filtered)))
    got shouldBe expectedFilter
    sink.visitCount should be > 1L

    ScarletRuntime.get.shuffle.clear()
    withConf(batchSize = 2, threads = 4, partitions = 1)
    shows(columnar(filtered)) shouldBe expectedFilter
    shows(columnar(mapped)) shouldBe expectedMap
  }

  "nullable morsels" should "keep the null call counts when the batch is split across threads" in {
    val calls = new AtomicInteger
    val filtered = nullableJoin.filter { (row: (Int, (Int, Int))) =>
      calls.incrementAndGet()
      row._2._1 > 0
    }
    withConf(batchSize = 2, threads = 4, partitions = 1)

    shows(columnar(filtered)) shouldBe Seq("1,10,100", "5,4,0")
    calls.get() shouldBe 3
  }

  "a type-changing triple map" should "lower, then decline before publish and fall back to rows" in {
    val plan = Plan.MapOp(
      presentJoin.plan,
      (row: (Int, (Int, Int))) => (row._1.toLong, (row._2._1, row._2._2)),
    )
    val expected = row(DistCollection(plan).collect().toList)
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail("type-changing map was refused by lower"))
    lowered.shape shouldBe "Project(HashJoin(Exchange(Scan), Exchange(Scan)))"

    val input = denseRow
    val sink = new CountingSink
    val kernel = ColumnMetrics.withSink(sink) {
      intercept[ColumnarFallback] {
        PairKernel.mapTriple(input, (_, _, _) => java.lang.Long.valueOf(1L))
      }
    }
    sink.batchCount shouldBe 0L
    kernel.getMessage should include("triple map did not stay (Int, (Int, Int))")
    kernel.getMessage should include("java.lang.Long")

    val caught = intercept[ColumnarFallback] {
      ColumnarInterpreter.interpret(plan, lowered)
    }
    caught.getMessage should include("triple map did not stay (Int, (Int, Int))")
    caught.getMessage should include("java.lang.Long")
    ColumnarPlanner.tryRun(plan) shouldBe Option.empty[Seq[Partition[_]]]
    ScarletRuntime.get.shuffle.clear()
    DistCollection(plan).collect().toList shouldBe expected
  }

  "chained whole-row filters" should "match the row path without using the slot compaction" in {
    val filtered = presentJoin
      .filter((row: (Int, (Int, Int))) => row._2._1 > 20)
      .filter((row: (Int, (Int, Int))) => row._2._2 < 700)

    ColumnarPlanner.lower(filtered.plan).map(_.shape) shouldBe Some(
      "FilterTripleRow(FilterTripleRow(HashJoin(Exchange(Scan), Exchange(Scan))))",
    )
    shows(columnar(filtered)) shouldBe shows(row(filtered.collect().toList))
  }

  "sort-merge and broadcast" should "run a whole-row filter and a same-type map" in {
    val left = DistCollection.ofIntPairs(Array(1, 2, 3), Array(10, 20, 30), 1)
    val right = DistCollection.ofIntPairs(Array(1, 2, 3), Array(100, 200, 300), 1)
    val broadcast = left.broadcastJoin(right)
    val merged = left.sortMergeJoin(right)

    List(broadcast, merged).foreach { joined =>
      val filtered = joined.filter((row: (Int, (Int, Int))) => row._2._1 > 10)
      val mapped = joined.map((row: (Int, (Int, Int))) => (row._1, (row._2._1 + 1, row._2._2)))
      shows(columnar(filtered)) shouldBe shows(row(filtered.collect().toList))
      shows(columnar(mapped)) shouldBe shows(row(mapped.collect().toList))
    }
  }

  "a whole-row filter under a slot filter" should "drop the null before the slot predicate" in {
    val calls = new AtomicInteger
    val planned = nullableJoin
      .filter { (row: (Int, (Int, Int))) =>
        calls.incrementAndGet()
        row._2._1 > 0
      }
      .filterJoinKey(_ >= 0)

    // The whole-row filter is the inner step. It does not offer a null component. Keys 1, 3, and
    // 5 are present, and key 3's left zero is rejected. The slot filter then keeps both survivors.
    shows(columnar(planned)) shouldBe Seq("1,10,100", "5,4,0")
    calls.get() shouldBe 3
  }

  "a slot filter under a whole-row filter" should "publish a stored zero before that predicate" in {
    val calls = new AtomicInteger
    val planned = nullableJoin
      .filterJoinKey(_ >= 0)
      .filter { (row: (Int, (Int, Int))) =>
        calls.incrementAndGet()
        row._2._1 > 0
      }

    // The slot filter is the inner step, so it publishes the stored zero first. The whole-row
    // predicate then sees five present rows. Key 2's null left is zero and is rejected. Key 4's
    // null right is zero and its left 8 is kept.
    shows(columnar(planned)) shouldBe Seq("1,10,100", "4,8,0", "5,4,0")
    calls.get() shouldBe 5
  }

  "a slot filter" should "keep rejecting a null component instead of preserving it" in {
    val keyed = nullableJoin.filterJoinKey(_ >= 0)

    ColumnarPlanner.lower(keyed.plan).map(_.shape) shouldBe Some(
      "FilterTripleKey(HashJoin(Exchange(Scan), Exchange(Scan)))",
    )
    val got = shows(columnar(keyed))
    got.exists(_.contains("null")) shouldBe false
    got should contain("2,0,20")
    got should contain("4,8,0")
    shows(row(keyed.collect().toList)).exists(_.contains("null")) shouldBe true
  }

  "outer joins and long triples" should "stay on the row path" in {
    val left = DistCollection.ofIntPairs(Array(1), Array(10), 1)
    val right = DistCollection.ofIntPairs(Array(1), Array(100), 1)
    val outer = left.leftOuterJoin(right)
    val outerFilter = outer.filter((row: (Int, (Int, Option[Int]))) => row._2._2.isDefined)
    val outerMap = outer.map((row: (Int, (Int, Option[Int]))) => (row._1, row._2._1))
    val longJoin = DistCollection
      .ofLongPairs(Array(1L), Array(10L), 1)
      .join(DistCollection.ofLongPairs(Array(1L), Array(100L), 1))
    val longMap = longJoin.map((row: (Long, (Long, Long))) => (row._1, (row._2._1, row._2._2)))

    ColumnarPlanner.lower(outerFilter.plan) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.lower(outerMap.plan) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.lower(longMap.plan) shouldBe Option.empty[PhysicalOp]
    outerFilter.collect().toList shouldBe row(outerFilter.collect().toList)
  }

  "the triple kernels" should "honor nulls, a present zero, and a morsel range" in {
    val batch = mixedBatch
    val filterCalls = new AtomicInteger
    val filtered = PairKernel.filterTripleRow(
      batch,
      (key, left, _) => {
        filterCalls.incrementAndGet()
        key > 0 && left > 0
      },
    )
    filterCalls.get() shouldBe 2
    BatchCodec.decodeIntTriples(filtered) shouldBe Seq((1, (10, 100)))

    val acceptZero = new AtomicInteger
    val zeros = PairKernel.filterTripleRow(
      batch,
      (_, _, right) => {
        acceptZero.incrementAndGet()
        right == 0
      },
    )
    acceptZero.get() shouldBe 2
    BatchCodec.decodeIntTriples(zeros) shouldBe Seq((0, (5, 0)))

    val mapCalls = new AtomicInteger
    val mapped = PairKernel.mapTriple(
      batch,
      (key, left, right) => {
        mapCalls.incrementAndGet()
        if (left == 0) then nullLeft(key, right) else (key, (left + 1, right + 1))
      },
    )
    mapCalls.get() shouldBe 2
    shows(BatchCodec.decodeIntTriplesPreservingAbsence(mapped)) shouldBe Seq(
      "0,6,1",
      "1,11,101",
      "2,null,20",
      "4,0,null",
    )

    val rangeCalls = new AtomicInteger
    val range = PairKernel.mapTriple(
      batch,
      1,
      2,
      (key, left, right) => {
        rangeCalls.incrementAndGet()
        (key, (left + 1, right + 1))
      },
    )
    rangeCalls.get() shouldBe 1
    range.length shouldBe 2
    shows(BatchCodec.decodeIntTriplesPreservingAbsence(range)) shouldBe Seq("0,6,1", "2,null,20")

    val emptyCalls = new AtomicInteger
    val empty = PairKernel.filterTripleRow(
      batch,
      0,
      0,
      (_, _, _) => {
        emptyCalls.incrementAndGet()
        true
      },
    )
    emptyCalls.get() shouldBe 0
    empty.length shouldBe 0
  }

  private def presentJoin: DistCollection[(Int, (Int, Int))] =
    val keys = Array(1, 2, 3, 4, 5, 6, 7, 8)
    DistCollection
      .ofIntPairs(keys, keys.map(_ * 10), 1)
      .join(DistCollection.ofIntPairs(keys, keys.map(_ * 100), 1))

  private def nullableJoin: DistCollection[(Int, (Int, Int))] =
    val left = DistCollection(
      Seq(
        pair(intRef(1), intRef(10)),
        pair(intRef(2), absent),
        pair(intRef(3), intRef(0)),
        pair(intRef(4), intRef(8)),
        pair(intRef(5), intRef(4)),
      ),
      1,
    )
    val right = DistCollection(
      Seq(
        pair(intRef(1), intRef(100)),
        pair(intRef(2), intRef(20)),
        pair(intRef(3), intRef(7)),
        pair(intRef(4), absent),
        pair(intRef(5), intRef(0)),
      ),
      1,
    )
    left.join(right)

  /** Rows: present, null left, present zero right, null right with a present zero left. */
  private def mixedBatch: ColumnBatch =
    tripleBatch(
      Array(1, 2, 0, 4),
      Array(10, 0, 5, 0),
      Array(100, 20, 0, 0),
      row => row != 1,
      row => row != 3,
    )

  private def denseRow: ColumnBatch =
    tripleBatch(Array(1), Array(10), Array(100), _ => true, _ => true)

  private def tripleBatch(
      keys: Array[Int],
      left: Array[Int],
      right: Array[Int],
      leftPresent: Int => Boolean,
      rightPresent: Int => Boolean,
  ): ColumnBatch =
    val rows = keys.length
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keys, Validity.allValid(rows), rows),
        Int32Column.of(left, Validity.pack(rows, leftPresent), rows),
        Int32Column.of(right, Validity.pack(rows, rightPresent), rows),
      ),
      rows,
    )

  private def columnar[A](collection: DistCollection[A]): Seq[A] =
    val ran = ColumnarPlanner
      .tryRun(collection.plan)
      .getOrElse(fail(s"stayed on the row path: ${collection.plan}"))
    ran.flatMap(_.data).toList

  private def withConf(batchSize: Int, threads: Int, partitions: Int): Unit =
    ScarletConf.set(
      original.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        threadPoolSize = threads,
        defaultShufflePartitions = partitions,
      ),
    )

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def pair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def nullLeft(key: Int, right: Int): (Int, (Int, Int)) =
    Tuple2(intRef(key), Tuple2(absent, intRef(right))).asInstanceOf[(Int, (Int, Int))]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absent: java.lang.Integer = null

  it should "apply a slot filter and a whole-row filter in one chain" in {
    val joined = presentJoin
    val chained = joined
      .filterJoinKey(_ > 0)
      .filter { case (_, (left, right)) => left + right > 15 }
    val actual = chained.collect().toSeq
    actual should contain theSameElementsAs row(chained.collect().toSeq)
    ColumnarPlanner.lower(chained.plan) shouldBe defined
  }

  private def intRef(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)

  private def shows(rows: Seq[(Int, (Int, Int))]): Seq[String] =
    rows.map(show).sorted

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Product",
    ),
  )
  private def show(row: (Int, (Int, Int))): String =
    row.productElement(1) match
      case nested: Product =>
        s"${cell(row.productElement(0))},${cell(nested.productElement(0))},${cell(nested.productElement(1))}"
      case _ => fail(s"expected a nested pair, got $row")

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def cell(value: Any): String = value match
    case null => "null"
    case present: Int => present.toString
    case other => fail(s"expected an int cell, got $other")
