package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.columnar.ColumnPipeline.JoinInput
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Int inner shuffle-hash join over a filter selection.
 *
 * A two-filter stack on either side keeps the survivor index. The exchange scatters those rows
 * and does not slice them. A null value on a dropped row stays on this path. A null value on a
 * selected row, or on a dense side beside a selection, declines. A null key is still dropped.
 */
class TestSelectionJoin extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val buildKeys = Array(1, 2, -1, 4, Int.MinValue, 1, 2, 1, 4)
  private val buildValues = Array(10, 20, 30, 40, 50, 11, 21, 12, 41)
  private val probeKeys = Array(2, -3, 4, 4, 1)
  private val probeValues = Array(100, 7, 200, 10, 300)
  private val filteredMatches = Seq(
    (2, (21, 100)),
    (4, (40, 200)),
    (4, (41, 200)),
  )
  private val probeOnlyMatches = Seq(
    (1, (10, 300)),
    (1, (11, 300)),
    (1, (12, 300)),
    (2, (20, 100)),
    (2, (21, 100)),
    (4, (40, 200)),
    (4, (41, 200)),
  )
  private val buildOnlyMatches = filteredMatches ++ Seq(
    (4, (40, 10)),
    (4, (41, 10)),
  )
  private val buildShape =
    "HashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(Scan))"
  private val probeShape =
    "HashJoin(Exchange(Scan), Exchange(FilterValue(FilterKey(Scan))))"
  private val bothShape =
    "HashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(FilterValue(FilterKey(Scan))))"

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 2,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "a two-filter inner join" should "match the row path and skip the compact on the build side" in {
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val build = buildPairs
    val probe = probePairs
    val filtered = build
      .filterKeys { (key: Int) =>
        val _ = keyCalls.incrementAndGet()
        key > 0
      }
      .filterValues { (value: Int) =>
        val _ = valueCalls.incrementAndGet()
        value > 20
      }
    val joined = filtered.join(probe)

    shape(joined) shouldBe buildShape
    val found = joined.collect().toSeq
    // One pass: the key predicate sees every build row, and the value predicate sees the seven
    // keys that passed. A second interpret would double both counts.
    keyCalls.get() shouldBe buildKeys.length
    valueCalls.get() shouldBe 7
    found should contain theSameElementsAs buildOnlyMatches
    found should contain theSameElementsAs row(joined.collect().toSeq)
    val hinted = filtered.shuffleHashJoin(probe)
    shape(hinted) shouldBe buildShape
    hinted.collect().toSeq should contain theSameElementsAs buildOnlyMatches
    assertFewerBatches(joined, build, probe, Some(filtered), None)
  }

  it should "match the row path and skip the compact on the probe side" in {
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    // Eight source partitions keep every build batch within the batch size, so the dense slice
    // publishes nothing and the batch-count gap is the probe compact.
    val build = DistCollection.ofIntPairs(buildKeys, buildValues, 8)
    val probe = probePairs
    val filtered = probe
      .filterKeys { (key: Int) =>
        val _ = keyCalls.incrementAndGet()
        key > 0
      }
      .filterValues { (value: Int) =>
        val _ = valueCalls.incrementAndGet()
        value > 50
      }
    val joined = build.join(filtered)

    shape(joined) shouldBe probeShape
    val found = joined.collect().toSeq
    keyCalls.get() shouldBe probeKeys.length
    valueCalls.get() shouldBe 4
    found should contain theSameElementsAs probeOnlyMatches
    found should contain theSameElementsAs row(joined.collect().toSeq)
    assertFewerBatches(joined, build, probe, None, Some(filtered))
  }

  it should "match the row path and skip the compact on both sides" in {
    val buildKeyCalls = new AtomicInteger
    val buildValueCalls = new AtomicInteger
    val probeKeyCalls = new AtomicInteger
    val probeValueCalls = new AtomicInteger
    val build = buildPairs
    val probe = probePairs
    val filteredBuild = build
      .filterKeys { (key: Int) =>
        val _ = buildKeyCalls.incrementAndGet()
        key > 0
      }
      .filterValues { (value: Int) =>
        val _ = buildValueCalls.incrementAndGet()
        value > 20
      }
    val filteredProbe = probe
      .filterKeys { (key: Int) =>
        val _ = probeKeyCalls.incrementAndGet()
        key > 0
      }
      .filterValues { (value: Int) =>
        val _ = probeValueCalls.incrementAndGet()
        value > 50
      }
    val joined = filteredBuild.join(filteredProbe)

    shape(joined) shouldBe bothShape
    val found = joined.collect().toSeq
    buildKeyCalls.get() shouldBe buildKeys.length
    buildValueCalls.get() shouldBe 7
    probeKeyCalls.get() shouldBe probeKeys.length
    probeValueCalls.get() shouldBe 4
    found should contain theSameElementsAs filteredMatches
    found should contain theSameElementsAs row(joined.collect().toSeq)
    assertFewerBatches(joined, build, probe, Some(filteredBuild), Some(filteredProbe))
  }

  it should "leave a two-filter broadcast join on the broadcast node" in {
    val joined = keepBuild(buildPairs).broadcastJoin(probePairs)

    shape(joined) shouldBe "BroadcastJoin(FilterValue(FilterKey(Scan)), Scan)"
    val found = joined.collect().toSeq
    found should contain theSameElementsAs buildOnlyMatches
    found should contain theSameElementsAs row(joined.collect().toSeq)
  }

  it should "keep a null value on a dropped row" in {
    val keyCalls = new AtomicInteger
    val narrowCalls = new AtomicInteger
    // The first row witnesses `(Int, Int)`. The null value shares that batch and fails the key
    // filter, so the selection never reads it.
    val build = DistCollection(
      Seq((1, 10), erased(Some(0), None), (4, 40)),
      2,
    ).filterKeys { (key: Int) =>
      val _ = keyCalls.incrementAndGet()
      key > 0
    }.filterKeys { (key: Int) =>
      val _ = narrowCalls.incrementAndGet()
      key < 100
    }
    val joined = build.join(DistCollection.ofIntPairs(Array(1, 4), Array(100, 8), 2))
    val expected = Seq((1, (10, 100)), (4, (40, 8)))

    shape(joined) shouldBe "HashJoin(Exchange(FilterKey(FilterKey(Scan))), Exchange(Scan))"
    val found = joined.collect().toSeq
    keyCalls.get() shouldBe 3
    narrowCalls.get() shouldBe 2
    found should contain theSameElementsAs expected
    found should contain theSameElementsAs row(joined.collect().toSeq)
    ColumnarPlanner.tryRun(joined.plan).isDefined shouldBe true
  }

  it should "refuse a null value on a selected row" in {
    val keyCalls = new AtomicInteger
    val narrowCalls = new AtomicInteger
    val build = DistCollection(
      Seq((1, 10), erased(Some(2), None)),
      2,
    ).filterKeys { (key: Int) =>
      val _ = keyCalls.incrementAndGet()
      key > 0
    }.filterKeys { (key: Int) =>
      val _ = narrowCalls.incrementAndGet()
      key < 100
    }
    val joined = build.join(DistCollection.ofIntPairs(Array(1, 2), Array(100, 200), 2))

    shape(joined) shouldBe "HashJoin(Exchange(FilterKey(FilterKey(Scan))), Exchange(Scan))"
    an[ColumnarFallback] should be thrownBy run(joined)
    keyCalls.get() shouldBe 2
    narrowCalls.get() shouldBe 2
    ColumnarPlanner.tryRun(joined.plan) shouldBe None
    val found = shown(joined.collect())
    found should contain theSameElementsAs shown(row(joined.collect()))
    found should contain("2,absent,200")
    found should contain("1,10,100")
  }

  it should "refuse a null value on the dense side of a mixed join" in {
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val build = DistCollection
      .ofIntPairs(Array(1, 4), Array(10, 40), 2)
      .filterKeys { (key: Int) =>
        val _ = keyCalls.incrementAndGet()
        key > 0
      }
      .filterValues { (value: Int) =>
        val _ = valueCalls.incrementAndGet()
        value > 0
      }
    val probe = DistCollection(Seq((1, 100), erased(Some(4), None)), 2)
    val joined = build.join(probe)

    shape(joined) shouldBe buildShape
    an[ColumnarFallback] should be thrownBy run(joined)
    keyCalls.get() shouldBe 2
    valueCalls.get() shouldBe 2
    ColumnarPlanner.tryRun(joined.plan) shouldBe None
  }

  it should "drop a null key on a selected row" in {
    val build = DistCollection(
      Seq((1, 10), erased(None, Some(99)), (4, 40)),
      2,
    ).filterValues((_: Int) => true).filterValues((value: Int) => value > 0)
    val probe = DistCollection(
      Seq((1, 100), erased(None, Some(7)), (4, 8)),
      2,
    )
    val joined = build.join(probe)
    val expected = Seq((1, (10, 100)), (4, (40, 8)))

    shape(joined) shouldBe "HashJoin(Exchange(FilterValue(FilterValue(Scan))), Exchange(Scan))"
    val found = joined.collect().toSeq
    found should contain theSameElementsAs expected
    found should contain theSameElementsAs row(joined.collect().toSeq)
    ColumnarPlanner.tryRun(joined.plan).isDefined shouldBe true
  }

  it should "join nothing from an empty selection" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq((Some(1), None), (Some(2), Some(5))),
    )
    val empty = Selected(batch, Array(0, 1), 0)
    val probe = BatchCodec.intPairs(Seq((1, 100)))
    val parts = ColumnPipeline.innerJoin(
      JoinInput.Selected(Vector(empty)),
      JoinInput.Dense(Vector(probe)),
      numPartitions = 1,
      parallelism = 1,
    )

    ColumnPipeline.valuesFullyPresent(Vector(empty)) shouldBe true
    parts.map(_.length).sum shouldBe 0
  }

  it should "drop every row when both filters reject the batch" in {
    val joined = buildPairs
      .filterKeys((_: Int) => false)
      .filterValues((_: Int) => true)
      .join(probePairs)

    shape(joined) shouldBe buildShape
    joined.collect() shouldBe empty
    joined.collect() shouldBe row(joined.collect())
    published(joined.collect()) should be > 0L
  }

  it should "join an empty source" in {
    val joined = DistCollection
      .ofIntPairs(Array.empty[Int], Array.empty[Int], 2)
      .filterKeys((_: Int) => true)
      .filterValues((_: Int) => true)
      .join(DistCollection.ofIntPairs(Array(1), Array(2), 2))

    shape(joined) shouldBe buildShape
    joined.collect() shouldBe empty
    joined.collect() shouldBe row(joined.collect())
    published(joined.collect()) should be > 0L
  }

  it should "keep every row when both filters pass" in {
    val build = DistCollection.ofIntPairs(Array(1, 2, -1), Array(10, 20, 30), 2)
    val probe = DistCollection.ofIntPairs(Array(1, -1, 3), Array(100, 300, 9), 2)
    val joined = build
      .filterKeys((_: Int) => true)
      .filterValues((_: Int) => true)
      .join(probe)
    val expected = Seq((1, (10, 100)), (-1, (30, 300)))

    shape(joined) shouldBe buildShape
    val found = joined.collect().toSeq
    found should contain theSameElementsAs expected
    found should contain theSameElementsAs build.join(probe).collect().toSeq
    found should contain theSameElementsAs row(joined.collect().toSeq)
  }

  it should "join one hot key across morsels" in {
    val rows = 20
    val build = DistCollection
      .ofIntPairs(Array.fill(rows)(7), Array.tabulate(rows)(_ + 1), 2)
      .filterKeys((key: Int) => key == 7)
      .filterValues((value: Int) => value > 0)
    val joined = build.join(DistCollection.ofIntPairs(Array(7), Array(100), 2))
    val expected = (1 to rows).map(value => (7, (value, 100)))

    shape(joined) shouldBe buildShape
    val found = joined.collect().toSeq
    found should contain theSameElementsAs expected
    found should contain theSameElementsAs row(joined.collect().toSeq)
    published(joined.collect()) should be > 0L
  }

  it should "emit the cartesian product of duplicate keys" in {
    val build = DistCollection
      .ofIntPairs(Array(1, 1, 2), Array(10, 11, 20), 2)
      .filterKeys((key: Int) => key > 0)
      .filterValues((value: Int) => value > 0)
    val probe = DistCollection
      .ofIntPairs(Array(1, 1, 3), Array(100, 101, 7), 2)
      .filterKeys((key: Int) => key > 0)
      .filterValues((value: Int) => value > 0)
    val joined = build.join(probe)
    val expected = Seq(
      (1, (10, 100)),
      (1, (10, 101)),
      (1, (11, 100)),
      (1, (11, 101)),
    )

    shape(joined) shouldBe bothShape
    val found = joined.collect().toSeq
    found should contain theSameElementsAs expected
    found should contain theSameElementsAs row(joined.collect().toSeq)
  }

  "JoinInput" should "read only the selected prefix" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq((Some(5), Some(50)), (None, Some(10)), (Some(1), Some(20))),
    )
    val selected = Selected(batch, Array(1, 2), 2)
    val probe = BatchCodec.intPairs(Seq((5, 500), (1, 100)))
    val parts = ColumnPipeline.innerJoin(
      JoinInput.Selected(Vector(selected)),
      JoinInput.Dense(Vector(probe)),
      numPartitions = 1,
      parallelism = 1,
    )
    val gathered = ColumnPipeline.innerJoin(
      Vector(Selection.materialize(selected)),
      Vector(probe),
      numPartitions = 1,
      parallelism = 1,
      batchSize = 8,
    )

    ColumnPipeline.valuesFullyPresent(Vector(selected)) shouldBe true
    triples(parts) should contain theSameElementsAs Seq((1, (20, 100)))
    triples(parts) should contain theSameElementsAs triples(gathered)
  }

  it should "treat a null outside the prefix as present and a selected null as absent" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq((Some(1), None), (Some(2), Some(5)), (None, Some(9))),
    )
    val dropped = Selected(batch, Array(1), 1)
    val selectedNull = Selected(batch, Array(0), 1)
    val empty = Selected(batch, Array(0, 1, 2), 0)

    ColumnPipeline.valuesFullyPresent(Vector(dropped)) shouldBe true
    ColumnPipeline.valuesFullyPresent(Vector(selectedNull)) shouldBe false
    ColumnPipeline.valuesFullyPresent(Vector(empty)) shouldBe true
    ColumnPipeline.valuesFullyPresent(Vector.empty[Selected]) shouldBe true
  }

  private def buildPairs: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(buildKeys, buildValues, 4)

  private def probePairs: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(probeKeys, probeValues, 4)

  private def keepBuild(source: DistCollection[(Int, Int)]): DistCollection[(Int, Int)] =
    source.filterKeys((key: Int) => key > 0).filterValues((value: Int) => value > 20)

  private def shape(data: DistCollection[_]): String =
    ColumnarPlanner.lower(data.plan).map(_.shape).getOrElse(fail("expected a columnar plan"))

  private def run(data: DistCollection[_]): Unit =
    val op = ColumnarPlanner.lower(data.plan).getOrElse(fail("expected a columnar plan"))
    val _ = ColumnarInterpreter.interpret(data.plan, op)

  /**
   * The dense baseline compacts the filter, then slices and joins. The selection collect skips
   * that compact. The two batch counts differ by the compact alone.
   */
  private def assertFewerBatches(
      joined: DistCollection[(Int, (Int, Int))],
      build: DistCollection[(Int, Int)],
      probe: DistCollection[(Int, Int)],
      filteredBuild: Option[DistCollection[(Int, Int)]],
      filteredProbe: Option[DistCollection[(Int, Int)]],
  ): Unit =
    val encode = published {
      val _ = pairBatches(build)
      val _ = pairBatches(probe)
    }
    val selectedCount = published {
      ScarletRuntime.get.shuffle.clear()
      joined.collect()
    }
    val (afterFilter, dense) = ColumnMetrics.withSink(new CountingSink) {
      val buildBatches = pairBatches(filteredBuild.getOrElse(build))
      val probeBatches = pairBatches(filteredProbe.getOrElse(probe))
      val mid = ColumnMetrics.batchCount
      val _ = ColumnPipeline.innerJoin(
        buildBatches,
        probeBatches,
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ColumnarPlanner.batchSize,
      )
      (mid, ColumnMetrics.batchCount)
    }
    val compactPublishes = afterFilter - encode
    selectedCount should be > 0L
    compactPublishes should be > 0L
    (dense - selectedCount) shouldBe compactPublishes

  private def pairBatches(data: DistCollection[(Int, Int)]): Vector[ColumnBatch] =
    val op = ColumnarPlanner.lower(data.plan).getOrElse(fail("expected a pair plan"))
    ColumnarInterpreter.pairsOf(ColumnarInterpreter.interpret(data.plan, op))

  private def triples(batches: Vector[ColumnBatch]): Seq[(Int, (Int, Int))] =
    batches.filter(_.length > 0).flatMap(batch => BatchCodec.decodeIntTriples(batch))

  private def published[A](body: => A): Long =
    ColumnMetrics.withSink(new CountingSink) {
      val _ = body
      ColumnMetrics.batchCount
    }

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  /**
   * Named erasure boundary. The tuple has to carry a null component. `productElement` reads it
   * without unboxing.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erased(key: Option[Int], value: Option[Int]): (Int, Int) =
    val left: AnyRef = key match
      case Some(present) => Int.box(present)
      case None => null
    val right: AnyRef = value match
      case Some(present) => Int.box(present)
      case None => null
    Tuple2(left, right).asInstanceOf[(Int, Int)]

  /**
   * Named erasure boundary. A join row may hold a null value. `productElement` reads it without
   * unboxing. `absent` is that null.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def shown(rows: Iterable[Product]): Seq[String] =
    rows.toSeq.map { row =>
      val values = row.productElement(1).asInstanceOf[Product]
      Seq(row.productElement(0), values.productElement(0), values.productElement(1))
        .map {
          case null => "absent"
          case value: Int => String.valueOf(value)
          case _ => "other"
        }
        .mkString(",")
    }
