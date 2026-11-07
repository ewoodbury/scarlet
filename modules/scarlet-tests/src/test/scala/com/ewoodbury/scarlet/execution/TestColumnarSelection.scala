package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.{DictionaryStringPredicate, DistCollection}
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{ExecutionService, Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Pins which plans select the columnar kernels, and that the row path still owns everything else.
 */
class TestColumnarSelection extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()

  override def afterEach(): Unit =
    ColumnMetrics.restore(NoOpSink)
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "int filter" should "keep input order, partition count, and a columnar layout" in {
    val filtered = DistCollection(Seq(1, 2, 3, 4, 5), 2).filter(_ % 2 == 0)
    filtered.collect() shouldBe Seq(2, 4)
    partitionsOf(filtered).size shouldBe 2
    assertNarrow(filtered, 2)
  }

  it should "keep order when map and filter run as morsels" in {
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 2, threadPoolSize = 4))
    val mapped = DistCollection(Seq(1, 2, 3, 4, 5), 1).map(_ * 2).filter(_ > 5)
    mapped.collect() shouldBe Seq(6, 8, 10)
    partitionsOf(mapped).size shouldBe 1
    assertNarrow(mapped, 1)
  }

  it should "stay columnar and empty when every row is dropped" in {
    val dropped = DistCollection(Seq(1, 2, 3), 3).filter(_ > 10)
    dropped.collect() shouldBe empty
    val parts = partitionsOf(dropped)
    parts.size shouldBe 3
    parts.forall(_.data.isEmpty) shouldBe true
    assertNarrow(dropped, 3)
  }

  it should "classify ints when some source partitions are empty" in {
    val source = Plan.Source(
      Seq(
        Partition(Seq.empty[Int]),
        Partition(Seq(4, 5)),
        Partition(Seq.empty[Int]),
      ),
    )
    val filtered = Plan.FilterOp(source, (value: Int) => value > 0)
    val info = ColumnarPlanner.partitioning(filtered).getOrElse(fail("empty partitions hid the ints"))
    info.distribution shouldBe Distribution.Unknown(3)
    info.layout shouldBe Layout.Columnar(ScarletConf.get.columnarBatchSize)
    DistCollection(filtered).collect() shouldBe Seq(4, 5)
    partitionsOf(DistCollection(filtered)).map(_.data.toSeq) shouldBe Seq(
      Seq.empty[Int],
      Seq(4, 5),
      Seq.empty[Int],
    )
  }

  "int map to string" should "return strings on the row path" in {
    DistCollection(Seq(1, 2, 3), 2).map(_.toString).collect() shouldBe Seq("1", "2", "3")
  }

  "string keys" should "lower groupByKey and hash reduceByKey and join" in {
    val grouped = DistCollection(Seq("a" -> 1, "a" -> 2, "b" -> 3), 2).groupByKey
    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some("GroupByKey(Exchange(Scan))")
    val found = grouped.collect().map { case (key, values) => key -> values.toSeq }.toMap
    val saved = ScarletConf.get
    val expected =
      try
        ScarletRuntime.get.shuffle.clear()
        ScarletConf.set(saved.copy(columnarExecution = false))
        grouped.collect().map { case (key, values) => key -> values.toSeq }.toMap
      finally
        ScarletRuntime.get.shuffle.clear()
        ScarletConf.set(saved)
    found shouldBe expected
    found shouldBe Map("a" -> Seq(1, 2), "b" -> Seq(3))

    val reduced = DistCollection(Seq("a" -> 1, "a" -> 2, "b" -> 3), 2).reduceByKey[String, Int](_ + _)
    reduced.collect().toMap shouldBe Map("a" -> 3, "b" -> 3)
    val reducedInfo = infoOf(reduced).getOrElse(fail("string reduce stayed on the row path"))
    reducedInfo.layout shouldBe Layout.StringPair(ScarletConf.get.columnarBatchSize)
    reducedInfo.distribution shouldBe Distribution.Hash(
      ScarletConf.get.defaultShufflePartitions,
      PartitioningInfo.RowKeyOrdinals,
    )
    ColumnarPlanner.lower(reduced.plan).map(_.shape) shouldBe Some(
      "StringHashAggregate(Exchange(Scan))",
    )

    val joined = DistCollection(Seq("a" -> 1), 1).join(DistCollection(Seq("a" -> 10), 1))
    joined.collect().toSeq should contain theSameElementsAs Seq("a" -> ((1, 10)))
    val joinedInfo = infoOf(joined).getOrElse(fail("string join stayed on the row path"))
    joinedInfo.layout shouldBe Layout.StringTriple(ScarletConf.get.columnarBatchSize)
    ColumnarPlanner.lower(joined.plan).map(_.shape) shouldBe Some(
      "StringHashJoin(Exchange(Scan), Exchange(Scan))",
    )
  }

  "union of int maps" should "concatenate both sides columnarly" in {
    val left = DistCollection(Seq(1, 2, 3), 2).map(_ * 2)
    val right = DistCollection(Seq(4, 5), 1).filter(_ % 2 == 1)
    val united = left.union(right)
    united.collect() shouldBe Seq(2, 4, 6, 5)
    infoOf(united).map(_.numPartitions) shouldBe Some(3)
    assertNarrow(left, 2)
  }

  "int reduceByKey" should "hash into the shuffle width and apply the user operator" in {
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 2, threadPoolSize = 4))
    val rows = Seq(0 -> 1, 0 -> 2, 4 -> 3, 1 -> Int.MaxValue, 1 -> 10, Int.MinValue -> 8, Int.MinValue -> -3)
    val reduced = DistCollection(rows, 3).reduceByKey[Int, Int](_ + _)
    val width = ScarletConf.get.defaultShufflePartitions

    reduced.collect().toMap shouldBe Map(
      0 -> 3,
      4 -> 3,
      1 -> (Int.MaxValue + 10),
      Int.MinValue -> 5,
    )
    val parts = partitionsOf(reduced)
    parts.size shouldBe width
    parts.exists(_.data.isEmpty) shouldBe true
    assertBuckets(parts)
    assertUniqueKeys(parts)
    assertHashed(reduced)
  }

  it should "apply max rather than a sum" in {
    val rows = Seq(1 -> 3, 1 -> 9, 1 -> 4, 2 -> 1, 2 -> 8)
    val reduced = DistCollection(rows, 2).reduceByKey[Int, Int](math.max)
    reduced.collect().toMap shouldBe Map(1 -> 9, 2 -> 8)
    assertHashed(reduced)
  }

  it should "keep a hash layout through a later filter" in {
    val filtered = DistCollection(Seq(1 -> 1, 2 -> 2, 1 -> 3, 5 -> 1), 2)
      .reduceByKey[Int, Int](_ + _)
      .filter(_._2 > 1)
    filtered.collect().toMap shouldBe Map(1 -> 4, 2 -> 2)
    assertHashed(filtered)
    val parts = partitionsOf(filtered)
    assertBuckets(parts)
    assertUniqueKeys(parts)
  }

  "int pairs" should "filter keys and values in input order" in {
    val pairs = DistCollection(Seq(1 -> 10, 2 -> 20, 3 -> 5, 4 -> 1), 2)
    pairs.filterKeys[Int, Int](_ % 2 == 0).collect() shouldBe Seq(2 -> 20, 4 -> 1)
    pairs.filterValues[Int, Int](_ > 5).collect() shouldBe Seq(1 -> 10, 2 -> 20)
    pairs.filter { case (_, value) => value > 5 }.collect() shouldBe Seq(1 -> 10, 2 -> 20)
    assertNarrow(pairs.filterKeys[Int, Int](_ > 0), 2)
  }

  "pair filter count" should "count two filters, one filter, and a row filter" in {
    val pairs = DistCollection(Seq(1 -> 10, 2 -> 20, 3 -> 5, 4 -> 1), 2)
    val both = pairs.filterKeys[Int, Int](_ % 2 == 0).filterValues[Int, Int](_ > 5)
    val keys = pairs.filterKeys[Int, Int](_ % 2 == 0)
    val rows = pairs.filter { case (key, value) => key > value }

    both.count() shouldBe 1L
    keys.count() shouldBe 2L
    rows.count() shouldBe 1L
    ColumnarPlanner.tryCount(both.plan) shouldBe Some(rowLong(both.count()))
    ColumnarPlanner.tryCount(keys.plan) shouldBe Some(rowLong(keys.count()))
    ColumnarPlanner.tryCount(rows.plan) shouldBe Some(rowLong(rows.count()))
    ColumnarPlanner.lower(both.plan).map(_.shape) shouldBe Some("FilterValue(FilterKey(Scan))")
    ColumnarPlanner.lower(keys.plan).map(_.shape) shouldBe Some("FilterKey(Scan)")
    ColumnarPlanner.lower(rows.plan).map(_.shape) shouldBe Some("FilterRow(Scan)")
  }

  it should "count an empty partition as zero and keep the other partition" in {
    val source = Plan.Source(
      Seq(
        Partition(Seq.empty[(Int, Int)]),
        Partition(Seq(2 -> 10, 1 -> 3)),
        Partition(Seq.empty[(Int, Int)]),
      ),
    )
    val filtered = Plan.FilterKeysOp(source, (key: Int) => key > 1)
    DistCollection(filtered).count() shouldBe 1L
    ColumnarPlanner.tryCount(filtered) shouldBe Some(1L)
    ColumnarPlanner.tryCount(filtered) shouldBe Some(rowLong(DistCollection(filtered).count()))
  }

  it should "rewrite a one-column value ordinal to index 0" in {
    val filtered = DistCollection(Seq(1 -> 10, 2 -> 3), 1).filterValues[Int, Int](_ > 5)
    val op = ColumnarPlanner.lower(filtered.plan).getOrElse(fail("value filter did not lower"))
    val compiled = PredicatePlan.compile(filtered.plan, op).getOrElse(fail("stack did not compile"))
    val wide = PredicatePlan.selectionSteps(compiled).getOrElse(fail("no wide steps"))
    val narrow = PredicatePlan
      .selectionSteps(PredicatePlan.encoded(compiled, Set(1)))
      .getOrElse(fail("no narrow steps"))

    stepOrdinal(wide) shouldBe 1
    stepOrdinal(narrow) shouldBe 0
  }

  it should "skip a null key and a null value without calling the predicate" in {
    val nullKeys = BatchCodec.nullableIntPairs(
      Seq((None, Some(1)), (Some(2), Some(3)), (Some(-1), Some(4)), (None, Some(5))),
    )
    val keySteps = Seq(Selection.Step.Int32(0, (key: Int) => key > 0))
    val keyCalls = new AtomicInteger
    val countedKeys = Seq(
      Selection.Step.Int32(
        0,
        (key: Int) => {
          keyCalls.incrementAndGet()
          key > 0
        },
      ),
    )
    counted(nullKeys, keySteps) shouldBe 1L
    Selection.count(nullKeys, 0, nullKeys.length, countedKeys)
    keyCalls.get() shouldBe 2

    val nullValues = BatchCodec.nullableIntPairs(
      Seq((Some(1), None), (Some(2), Some(0)), (Some(3), Some(4))),
    )
    val valueSteps = Seq(Selection.Step.Int32(1, (value: Int) => value >= 0))
    val valueCalls = new AtomicInteger
    val countedValues = Seq(
      Selection.Step.Int32(
        1,
        (value: Int) => {
          valueCalls.incrementAndGet()
          value >= 0
        },
      ),
    )
    // A stored zero on the null row would pass `>= 0` if the bit were ignored.
    counted(nullValues, valueSteps) shouldBe 2L
    Selection.count(nullValues, 0, nullValues.length, countedValues)
    valueCalls.get() shouldBe 2

    val keyOnNullValue = Seq(Selection.Step.Int32(0, (key: Int) => key > 0))
    counted(nullValues, keyOnNullValue) shouldBe 3L
  }

  it should "require both ordinals for a pair-row step" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq((None, Some(1)), (Some(4), None), (Some(2), Some(3)), (Some(1), Some(8))),
    )
    val calls = new AtomicInteger
    val steps = Seq(
      Selection.Step.IntPair((key, value) => {
        calls.incrementAndGet()
        key < value
      }),
    )

    counted(batch, Seq(Selection.Step.IntPair((key, value) => key < value))) shouldBe 2L
    Selection.count(batch, 0, batch.length, steps)
    calls.get() shouldBe 2
  }

  it should "count a dense window and a mixed range of the same batch" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq(
        (Some(2), Some(5)),
        (Some(4), Some(1)),
        (Some(6), Some(8)),
        (None, Some(9)),
        (Some(3), Some(7)),
      ),
    )
    val steps = Seq(
      Selection.Step.Int32(0, (key: Int) => key % 2 == 0),
      Selection.Step.Int32(1, (value: Int) => value > 4),
    )
    val calls = new AtomicInteger
    val countedSteps = Seq(
      Selection.Step.Int32(0, (key: Int) => key % 2 == 0),
      Selection.Step.Int32(
        1,
        (value: Int) => {
          calls.incrementAndGet()
          value > 4
        },
      ),
    )

    // Rows 1 and 2 are present, so the window is dense and starts mid-word.
    counted(batch, 1, 2, steps) shouldBe 1L
    counted(batch, steps) shouldBe 2L
    Selection.count(batch, 0, batch.length, countedSteps)
    // The null key fails the first step, so the value predicate sees three present keys.
    calls.get() shouldBe 3
  }

  it should "count a one-column batch from the only ordinal it reads" in {
    val keys = BatchCodec.ints(Seq(1, 2, 3, 4))
    val steps = Seq(Selection.Step.Int32(0, (key: Int) => key % 2 == 0))
    Selection.count(keys, 0, keys.length, steps) shouldBe 2L
    Selection.count(keys, 0, 0, steps) shouldBe 0L
  }

  "int join" should "match the pair multiset and the shuffle width" in {
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 2, threadPoolSize = 4))
    val build = DistCollection(Seq(1 -> 10, 1 -> 11, 2 -> 20, 4 -> 40), 2)
    val probe = DistCollection(Seq(1 -> 100, 2 -> 200, 1 -> 101, 3 -> 30), 2)
    val joined = build.join(probe)
    val triples = joined.collect().map { case (key, (left, right)) => (key, left, right) }.toSeq

    triples should contain theSameElementsAs Seq(
      (1, 10, 100),
      (1, 11, 100),
      (1, 10, 101),
      (1, 11, 101),
      (2, 20, 200),
    )
    val parts = partitionsOf(joined)
    parts.size shouldBe ScarletConf.get.defaultShufflePartitions
    assertBuckets(parts)
    assertHashed(joined)
  }

  it should "filter a later join result columnarly and preserve duplicate rows" in {
    val build = DistCollection(Seq(1 -> 10, 2 -> 20, 1 -> 10, 1 -> 11), 1)
    val probe = DistCollection(Seq(1 -> 7, 1 -> 8, 3 -> 9), 1)
    val filtered = build.join(probe).filterJoinLeft(_ == 10)
    val actual = columnar(filtered.collect()).toSeq

    actual should contain theSameElementsAs row {
      build.join(probe).filter { case (_, (left, _)) => left == 10 }.collect().toSeq
    }
    actual should contain theSameElementsAs Seq(
      1 -> ((10, 7)),
      1 -> ((10, 7)),
      1 -> ((10, 8)),
      1 -> ((10, 8)),
    )
    columnar {
      val info = assertHashed(filtered)
      info.layout shouldBe Layout.Columnar(ScarletConf.get.columnarBatchSize)
    }
  }

  it should "filter each supported join column" in {
    val build = DistCollection(Seq(1 -> 10, 2 -> 20, 1 -> 11), 1)
    val probe = DistCollection(Seq(1 -> 7, 2 -> 8, 1 -> 9), 1)
    val joined = build.join(probe)

    columnar(joined.filterJoinKey(_ == 1).collect()).toSeq should contain theSameElementsAs row {
      build.join(probe).filter { case (key, _) => key == 1 }.collect().toSeq
    }
    columnar(joined.filterJoinLeft(_ > 10).collect()).toSeq should contain theSameElementsAs row {
      build.join(probe).filter { case (_, (left, _)) => left > 10 }.collect().toSeq
    }
    columnar(joined.filterJoinRight(_ == 8).collect()).toSeq should contain theSameElementsAs row {
      build.join(probe).filter { case (_, (_, right)) => right == 8 }.collect().toSeq
    }
    columnar(assertHashed(joined.filterJoinKey(_ => true)))
  }

  it should "keep empty and all-surviving join filters columnar" in {
    val joined = DistCollection(Seq(1 -> 10, 2 -> 20), 1).join(
      DistCollection(Seq(1 -> 7, 2 -> 8), 1),
    )
    val emptyFilter = joined.filterJoinRight(_ < 0)
    val all = joined.filterJoinKey(_ => true)

    columnar(emptyFilter.collect()) shouldBe empty
    assertHashed(emptyFilter)
    columnar(all.collect()).toSeq should contain theSameElementsAs Seq(
      1 -> ((10, 7)),
      2 -> ((20, 8)),
    )
    assertHashed(all)
  }

  it should "filter a general later join predicate columnarly" in {
    val build = DistCollection(Seq(1 -> 10, 2 -> 20, 1 -> 11), 1)
    val probe = DistCollection(Seq(1 -> 7, 2 -> 8, 1 -> 9), 1)
    val filtered = build.join(probe).filter { case (_, (left, right)) => left + right > 15 }
    val actual = columnar(filtered.collect()).toSeq

    actual should contain theSameElementsAs row {
      build.join(probe).filter { case (_, (left, right)) => left + right > 15 }.collect().toSeq
    }
    columnar(infoOf(filtered)) shouldBe defined
  }

  it should "scan a joined triple once and publish only the compacted partitions" in {
    val conf = originalConf.copy(
      columnarExecution = true,
      columnarBatchSize = 1024,
      defaultShufflePartitions = 4,
      threadPoolSize = 2,
    )
    ScarletConf.set(conf)
    val build = DistCollection(Seq.fill(4)(1 -> 10), 1)
    val probe = DistCollection(Seq.fill(3)(1 -> 20), 1)
    val baseline = measuredJoin(build.join(probe))
    val predicateCalls = new java.util.concurrent.atomic.AtomicInteger(0)
    val filtered = build.join(probe).filterJoinKey { key =>
      predicateCalls.incrementAndGet()
      key == 1
    }
    val compacted = measuredJoin(filtered)

    baseline.count shouldBe 12L
    compacted.count shouldBe baseline.count
    predicateCalls.get() shouldBe baseline.count.toInt
    compacted.batches - baseline.batches shouldBe conf.defaultShufflePartitions.toLong
    compacted.visits - baseline.visits shouldBe conf.defaultShufflePartitions.toLong
  }

  "string filter" should "keep input order, nulls, and a dictionary layout" in {
    val filtered = DistCollection(texts(Some("a"), None, Some("bb"), Some("a"), Some("")), 2).filter {
      value =>
        Option(value) match
          case Some("bb") => false
          case _ => true
    }

    filtered.collect() shouldBe texts(Some("a"), None, Some("a"), Some(""))
    assertUtf8(filtered, 2)
    columnar(filtered.collect()) shouldBe row(filtered.collect())
  }

  it should "keep order when map and filter run as morsels" in {
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 2, threadPoolSize = 4))
    val mapped = DistCollection(Seq("a", "bb", "ccc", "d", "ee"), 1)
      .map(_.toUpperCase(java.util.Locale.ENGLISH))
      .filter(_.length > 1)

    mapped.collect() shouldBe Seq("BB", "CCC", "EE")
    assertUtf8(mapped, 1)
  }

  it should "select strings that start with null, and leave an all-null source on the row path" in {
    val witnessed =
      DistCollection(texts(None, Some("a"), Some("b")), 1).filter(value => Option(value).isDefined)
    witnessed.collect() shouldBe Seq("a", "b")
    assertUtf8(witnessed, 1)

    val unseen = DistCollection(texts(None, None), 2).filter(_ => true)
    unseen.collect() shouldBe texts(None, None)
    infoOf(unseen) shouldBe Option.empty[PartitioningInfo]
  }

  it should "filter string equality by dictionary code and match the scalar result" in {
    val values = texts(Some("US"), None, Some("CA"), Some("US"), Some("MX"))
    val actual = columnar(DistCollection(values, 2).filterStringEquals("US"))

    assertUtf8(actual, 2)
    actual.collect() shouldBe row(DistCollection(values, 2).filter(_ == "US").collect())
    actual.collect() shouldBe texts(Some("US"), Some("US"))

    val missing = columnar(DistCollection(values, 2).filterStringEquals("GB"))
    assertUtf8(missing, 2)
    missing.collect() shouldBe row(DistCollection(values, 2).filter(_ == "GB").collect())
    missing.collect() shouldBe empty
  }

  it should "filter a short string IN-list by dictionary code" in {
    val values = texts(Some("US"), None, Some("CA"), Some("MX"), Some("US"))
    val actual = columnar(DistCollection(values, 2).filterStringIn("CA", "US", "GB"))
    assertUtf8(actual, 2)
    val expected = row(DistCollection(values, 2).filter(value => Set("CA", "US", "GB").contains(value)))

    actual.collect() shouldBe expected.collect()
    actual.collect() shouldBe texts(Some("US"), Some("CA"), Some("US"))
  }

  it should "filter a dictionary predicate by its literals and keep startsWith on the scalar path" in {
    val equality = new DictionaryStringPredicate[String](Vector("US"), summon[String =:= String])
    val recognized = columnar(DistCollection(Seq("US", "CA", "US"), 1).filter(equality))

    recognized.collect() shouldBe Seq("US", "US")
    assertUtf8(recognized, 1)

    val scalarCalls = new java.util.concurrent.atomic.AtomicInteger(0)
    val startsWith = columnar {
      DistCollection(Seq("US", "CA", "USA"), 1).filter { value =>
        scalarCalls.incrementAndGet()
        value.startsWith("US")
      }
    }
    startsWith.collect() shouldBe Seq("US", "USA")
    scalarCalls.get() shouldBe 3
    assertUtf8(startsWith, 1)
  }

  it should "match the scalar path for an empty list, a null literal, and the eight-literal boundary" in {
    val values = texts(Some("a"), None, Some("b"), Some("c"))
    def source = DistCollection(values, 2)
    val eight = Vector("a", "b", "c", "d", "e", "f", "g", "h")
    val nine = eight :+ "i"

    def same(actual: DistCollection[String], keep: String => Boolean): Unit =
      columnar(actual.collect()) shouldBe row(source.filter(keep).collect())

    // Layout is read with columnar execution enabled. `same` leaves the row path selected.
    columnar(assertUtf8(source.filterStringIn(eight*), 2))
    same(source.filterStringIn(), _ => false)
    same(source.filterStringEquals(text(None)), BatchCodec.isAbsent)
    same(source.filterStringIn(eight*), value => eight.contains(value))
    same(source.filterStringIn(nine*), value => nine.contains(value))
    same(
      source.filterStringIn("a", text(None)),
      value =>
        value match
          case "a" => true
          case other => BatchCodec.isAbsent(other),
    )
  }

  it should "record a dictionary layout and match the row path when a map changes nulls" in {
    val plan = DistCollection(texts(Some("a"), None, Some("bb"), Some("a")), 2).map { value =>
      Option(value) match
        case None => "n"
        case Some("bb") => text(None)
        case Some(present) => present.toUpperCase(java.util.Locale.ENGLISH)
    }

    assertUtf8(plan, 2)
    columnar(plan.collect()) shouldBe row(plan.collect())
  }

  it should "fall back when a string map does not return a string" in {
    val mapped = DistCollection(Seq("a", "bb"), 2).map(_.length)

    mapped.collect() shouldBe Seq(1, 2)
  }

  "columnar path" should "match the row path on filter, map, reduceByKey, join, and filterKeys" in {
    val ints = Seq(1, 2, 3, 4, 5, 6)
    val pairs = Seq(1 -> 10, 2 -> 20, 1 -> 3, 4 -> 1, 2 -> 5)
    val build = Seq(1 -> 10, 1 -> 11, 2 -> 20, 4 -> 1)
    val probe = Seq(1 -> 100, 2 -> 200, 3 -> 30, 1 -> 101)

    def run[A](flag: Boolean)(body: => A): A =
      ScarletRuntime.get.shuffle.clear()
      ScarletConf.set(originalConf.copy(columnarExecution = flag))
      body

    run(true)(DistCollection(ints, 2).filter(_ % 2 == 0).collect()) shouldBe
      run(false)(DistCollection(ints, 2).filter(_ % 2 == 0).collect())
    run(true)(DistCollection(ints, 2).map(_ * 2).filter(_ > 5).collect()) shouldBe
      run(false)(DistCollection(ints, 2).map(_ * 2).filter(_ > 5).collect())
    run(true)(DistCollection(pairs, 3).reduceByKey[Int, Int](_ + _).collect().toMap) shouldBe
      run(false)(DistCollection(pairs, 3).reduceByKey[Int, Int](_ + _).collect().toMap)
    run(true)(DistCollection(pairs, 2).filterKeys[Int, Int](_ % 2 == 0).collect()) shouldBe
      run(false)(DistCollection(pairs, 2).filterKeys[Int, Int](_ % 2 == 0).collect())

    val columnarJoin = run(true) {
      DistCollection(build, 2).join(DistCollection(probe, 2)).collect().toSeq.sorted
    }
    val rowJoin = run(false) {
      DistCollection(build, 2).join(DistCollection(probe, 2)).collect().toSeq.sorted
    }
    columnarJoin shouldBe rowJoin
  }

  "columnarExecution" should "force the row path when it is false" in {
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val filtered = DistCollection(Seq(1, 2, 3, 4), 2).filter(_ % 2 == 0)
    filtered.collect() shouldBe Seq(2, 4)
    infoOf(filtered) shouldBe Option.empty[PartitioningInfo]
  }

  private def counted(batch: ColumnBatch, steps: Seq[Selection.Step]): Long =
    counted(batch, 0, batch.length, steps)

  private def counted(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      steps: Seq[Selection.Step],
  ): Long =
    val got = Selection.count(batch, start, length, steps)
    got shouldBe Selection(batch, start, length, steps).length.toLong
    got

  private def stepOrdinal(steps: Vector[Selection.Step]): Int = steps match
    case Vector(Selection.Step.Int32(ordinal, _)) => ordinal
    case other => fail(s"expected one int step, got $other")

  private def rowLong(body: => Long): Long =
    val saved = ScarletConf.get
    try
      ScarletRuntime.get.shuffle.clear()
      ScarletConf.set(saved.copy(columnarExecution = false))
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def columnar[A](body: => A): A =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf.copy(columnarExecution = true))
    body

  private def row[A](body: => A): A =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf.copy(columnarExecution = false))
    body

  private def partitionsOf[A](collection: DistCollection[A]): Seq[Partition[A]] =
    ExecutionService.get.executePartitions(collection.plan)

  private def infoOf[A](collection: DistCollection[A]): Option[PartitioningInfo] =
    ColumnarPlanner.partitioning(collection.plan)

  private def assertUtf8[A](collection: DistCollection[A], width: Int): PartitioningInfo =
    val info = PartitioningInfo(
      Distribution.Unknown(width),
      OrderingTag.Unsorted,
      Layout.Utf8Dict(ScarletConf.get.columnarBatchSize),
    )
    infoOf(collection) shouldBe Some(info)
    info

  private def assertNarrow[A](collection: DistCollection[A], width: Int): PartitioningInfo =
    val info = infoOf(collection).getOrElse(fail("expected a columnar plan"))
    info.distribution shouldBe Distribution.Unknown(width)
    info.ordering shouldBe OrderingTag.Unsorted
    info.layout shouldBe Layout.Columnar(ScarletConf.get.columnarBatchSize)
    info

  private def assertHashed[A](collection: DistCollection[A]): PartitioningInfo =
    val info = infoOf(collection).getOrElse(fail("expected a columnar plan"))
    info.distribution shouldBe Distribution.Hash(
      ScarletConf.get.defaultShufflePartitions,
      PartitioningInfo.RowKeyOrdinals,
    )
    info.ordering shouldBe OrderingTag.Unsorted
    info.layout shouldBe Layout.Columnar(ScarletConf.get.columnarBatchSize)
    info

  private final case class JoinFootprint(count: Long, batches: Long, visits: Long)

  private def measuredJoin[A](collection: DistCollection[A]): JoinFootprint =
    ScarletRuntime.get.shuffle.clear()
    ColumnMetrics.withSink(new CountingSink) {
      JoinFootprint(collection.count(), ColumnMetrics.batchCount, ColumnMetrics.visitCount)
    }

  private def assertBuckets[V](parts: Seq[Partition[(Int, V)]]): Unit =
    val width = parts.length
    parts.zipWithIndex.foreach { (partition, index) =>
      partition.data.foreach { (key, _) =>
        Math.floorMod(key, width) shouldBe index
      }
    }

  private def text(cell: Option[String]): String = BatchCodec.stringElement(cell)

  private def texts(cells: Option[String]*): Seq[String] = cells.map(text)

  private def assertUniqueKeys[V](parts: Seq[Partition[(Int, V)]]): Unit =
    val keys = parts.flatMap(_.data.map(_._1))
    keys.distinct.size shouldBe keys.size
    ()
