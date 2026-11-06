package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, LogicalType}
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Columnar `filter`, `keys`, `values`, and `mapValues` on `(Long, Long)`.
 *
 * The row path is the oracle for present rows. A null component is absent: `keys` and `values`
 * publish that absence, a filter drops the row without calling the predicate, and `mapValues`
 * skips a null value. Those null rules are checked on their own, because the row path unboxes a
 * null to zero.
 */
class TestLongPairOps extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "keys and values" should "match the row path for nulls, a present zero, and duplicate keys" in {
    val source = rows(mixed)
    val keyed = columnar(source.keys)
    val valued = columnar(source.values)

    keyed shouldBe row(source.keys.collect().toList)
    valued shouldBe row(source.values.collect().toList)
    // Index 1 is a present zero. Indexes 2 and 4 are null keys. Index 3 is a null value.
    // Read through AnyRef so a null Long is not unboxed to 0 before the check.
    longAt(keyed, 1) shouldBe 0L
    absentRef(slot(keyed, 2)) shouldBe true
    absentRef(slot(keyed, 4)) shouldBe true
    longAt(valued, 6) shouldBe 0L
    absentRef(slot(valued, 3)) shouldBe true
    absentRef(slot(valued, 4)) shouldBe true
    keyed.count(_ == 1L) shouldBe 2
    ColumnarPlanner.lower(source.keys.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.Longs)
    ColumnarPlanner.lower(source.values.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.Longs)
  }

  they should "match the row path for an empty partition" in {
    val empty = DistCollection.ofLongPairs(Array.empty[Long], Array.empty[Long], 2)
    val calls = new AtomicInteger
    val filtered = empty.filter { (pair: (Long, Long)) =>
      calls.incrementAndGet()
      pair._1 > 0L
    }
    val sink = new CountingSink
    val filteredRows = ColumnMetrics.withSink(sink)(columnar(filtered))

    filteredRows shouldBe Seq.empty[(Long, Long)]
    calls.get() shouldBe 0
    // One zero-length morsel per empty partition.
    sink.visitCount shouldBe 2L
    columnar(empty.keys) shouldBe row(empty.keys.collect().toList)
    columnar(empty.values) shouldBe row(empty.values.collect().toList)
    columnar(empty.mapValues[Long, Long, Long](_ + 1L)) shouldBe Seq.empty[(Long, Long)]
  }

  "filter" should "match the row path when a null unboxes to a rejected zero" in {
    val source = rows(mixed)
    val filtered = source.filter((pair: (Long, Long)) => pair._1 > 0L && pair._2 > 0L)
    val got = columnar(filtered)

    got shouldBe row(filtered.collect().toList)
    got shouldBe Seq(pair(longRef(1L), longRef(10L)), pair(longRef(1L), longRef(10L)))
    ColumnarPlanner.lower(filtered.plan).map(_.shape) shouldBe Some("FilterRow(Scan)")
    ColumnarPlanner.lower(filtered.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.LongPairs)
  }

  it should "drop a null component without calling the predicate, even when the predicate accepts zero" in {
    val source = rows(mixed)
    val calls = new AtomicInteger
    val filtered = source.filter { (_: (Long, Long)) =>
      calls.incrementAndGet()
      true
    }
    val got = columnar(filtered)

    calls.get() shouldBe 4
    got shouldBe Seq(
      pair(longRef(1L), longRef(10L)),
      pair(longRef(0L), longRef(5L)),
      pair(longRef(1L), longRef(10L)),
      pair(longRef(3L), longRef(0L)),
    )
    row(rows(mixed).filter((_: (Long, Long)) => true).collect().toList).length shouldBe mixed.length
  }

  it should "call the predicate once per present row, including a row it rejects" in {
    val source = rows(mixed)
    val calls = new AtomicInteger
    val filtered = source.filter { (pair: (Long, Long)) =>
      calls.incrementAndGet()
      pair._1 > 0L && pair._2 > 0L
    }

    columnar(filtered) shouldBe Seq(pair(longRef(1L), longRef(10L)), pair(longRef(1L), longRef(10L)))
    calls.get() shouldBe 4
  }

  "mapValues" should "match the row path for present values and a null key" in {
    val input = Seq(
      pair(longRef(1L), longRef(10L)),
      pair(absent, longRef(7L)),
      pair(longRef(0L), longRef(4L)),
      pair(longRef(1L), longRef(10L)),
    )
    val mapped = rows(input).mapValues[Long, Long, Long](_ + 1L)
    val got = columnar(mapped)

    got shouldBe row(mapped.collect().toList)
    val nullKey = pairAt(got, 1)
    absentRef(component(nullKey, 0)) shouldBe true
    longComponent(nullKey, 1) shouldBe 8L
    ColumnarPlanner.lower(mapped.plan).map(_.shape) shouldBe Some("MapValue(Scan)")
    ColumnarPlanner.lower(mapped.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.LongPairs)
  }

  it should "call the mapper once per present value and skip a null value" in {
    val source = rows(mixed)
    val calls = new AtomicInteger
    val mapped = source.mapValues[Long, Long, Long] { value =>
      calls.incrementAndGet()
      value + 1L
    }
    val got = columnar(mapped)

    // Five present values: the null key still has a value, and the two null values are skipped.
    calls.get() shouldBe 5
    got shouldBe Seq(
      pair(longRef(1L), longRef(11L)),
      pair(longRef(0L), longRef(6L)),
      pair(absent, longRef(8L)),
      pair(longRef(2L), absent),
      pair(absent, absent),
      pair(longRef(1L), longRef(11L)),
      pair(longRef(3L), longRef(1L)),
    )
    absentRef(component(pairAt(got, 3), 1)) shouldBe true
    absentRef(component(pairAt(got, 4), 1)) shouldBe true
  }

  "keys and values" should "publish one column and not read the other" in {
    val owned = DistCollection.ofLongPairs(Array(1L, 0L, 1L), Array(10L, 20L, 5L), 1)
    assertOneColumn(owned.keys, Set(0), Seq(1L, 0L, 1L))
    assertOneColumn(owned.values, Set(1), Seq(10L, 20L, 5L))

    val keySource = sideSource(9L, ordinal = 0)
    val valueSource = sideSource(9L, ordinal = 1)
    columnar(keySource.keys) shouldBe Seq(4L, 9L)
    columnar(valueSource.values) shouldBe Seq(8L, 9L)
  }

  "a small batch" should "match the row path across morsels" in {
    val source = wide
    withConf(batchSize = 2, threads = 1)
    val sink = new CountingSink
    val filtered = source.filter((pair: (Long, Long)) => pair._1 % 2L == 0L)
    val filteredRows = ColumnMetrics.withSink(sink)(columnar(filtered))
    val mapped = source.mapValues[Long, Long, Long](_ + 1L)
    val mapSink = new CountingSink
    val mappedRows = ColumnMetrics.withSink(mapSink)(columnar(mapped))

    filteredRows shouldBe row(filtered.collect().toList)
    mappedRows shouldBe row(mapped.collect().toList)
    sink.visitCount shouldBe 4L
    mapSink.visitCount shouldBe 4L
    columnar(source.keys) shouldBe row(source.keys.collect().toList)
    columnar(source.values) shouldBe row(source.values.collect().toList)
  }

  "several threads" should "match the row path for each op" in {
    val source = wide
    withConf(batchSize = 2, threads = 4)
    val filtered = source.filter((pair: (Long, Long)) => pair._2 > 0L)
    val mapped = source.mapValues[Long, Long, Long](_ + 1L)

    columnar(filtered) shouldBe row(filtered.collect().toList)
    columnar(mapped) shouldBe row(mapped.collect().toList)
    columnar(source.keys) shouldBe row(source.keys.collect().toList)
    columnar(source.values) shouldBe row(source.values.collect().toList)

    val nullable = rows(mixed)
    val nullFiltered = nullable.filter((pair: (Long, Long)) => pair._1 > 0L && pair._2 > 0L)
    val nullMapped = rows(nullKeyOnly).mapValues[Long, Long, Long](_ + 1L)
    columnar(nullable.keys) shouldBe row(nullable.keys.collect().toList)
    columnar(nullable.values) shouldBe row(nullable.values.collect().toList)
    columnar(nullFiltered) shouldBe row(nullFiltered.collect().toList)
    columnar(nullMapped) shouldBe row(nullMapped.collect().toList)
  }

  "filterKeys and filterValues" should "lower to the key and value kernels" in {
    val source = DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 1)
    val keyed = source.filterKeys[Long, Long](_ > 0L)
    val valued = source.filterValues[Long, Long](_ > 0L)

    ColumnarPlanner.lower(keyed.plan).map(_.shape) shouldBe Some("FilterKey(Scan)")
    ColumnarPlanner.lower(valued.plan).map(_.shape) shouldBe Some("FilterValue(Scan)")
    columnar(keyed) shouldBe row(keyed.collect().toList)
    columnar(valued) shouldBe row(valued.collect().toList)
  }

  private def wide: DistCollection[(Long, Long)] =
    DistCollection.ofLongPairs(
      Array(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L),
      Array(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L),
      1,
    )

  /** Present rows, a present zero, duplicate keys, a null key, a null value, and both null. */
  private def mixed: Seq[(Long, Long)] = Seq(
    pair(longRef(1L), longRef(10L)),
    pair(longRef(0L), longRef(5L)),
    pair(absent, longRef(7L)),
    pair(longRef(2L), absent),
    pair(absent, absent),
    pair(longRef(1L), longRef(10L)),
    pair(longRef(3L), longRef(0L)),
  )

  private def nullKeyOnly: Seq[(Long, Long)] = Seq(
    pair(longRef(1L), longRef(10L)),
    pair(absent, longRef(7L)),
    pair(longRef(1L), longRef(10L)),
  )

  private def rows(
      pairs: Seq[(Long, Long)],
      partitions: Int = 1,
  ): DistCollection[(Long, Long)] =
    DistCollection(pairs, partitions)

  private def columnar[A](collection: DistCollection[A]): Seq[A] =
    val ran = ColumnarPlanner
      .tryRun(collection.plan)
      .getOrElse(fail(s"stayed on the row path: ${collection.plan}"))
    ran.flatMap(_.data).toList

  private def assertOneColumn(
      collection: DistCollection[Long],
      ordinals: Set[Int],
      expected: Seq[Long],
  ): Unit =
    val op = ColumnarPlanner.lower(collection.plan).getOrElse(fail("projection stayed on the row path"))
    ColumnUse.ordinals(collection.plan, op) shouldBe ordinals
    val columns = ColumnarInterpreter.interpret(collection.plan, op)
    ColumnarInterpreter.batchesOf(columns).foreach { batch =>
      batch.columns.length shouldBe 1
      batch.schema shouldBe Vector(LogicalType.Int64)
    }
    columnar(collection) shouldBe expected

  private def withConf(batchSize: Int, threads: Int): Unit =
    ScarletConf.set(
      original.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        threadPoolSize = threads,
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
  private def pair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absent: java.lang.Long = null

  private def longRef(value: Long): java.lang.Long = java.lang.Long.valueOf(value)

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
    ),
  )
  private def component(row: (Long, Long), ordinal: Int): Any =
    row.productElement(ordinal)

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
    ),
  )
  private def absentRef(value: Any): Boolean =
    value.asInstanceOf[AnyRef] eq null

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def slot(values: Seq[Long], index: Int): AnyRef =
    values.asInstanceOf[Seq[AnyRef]].iterator.drop(index).next()

  private def longAt(values: Seq[Long], index: Int): Long = slot(values, index) match
    case value: java.lang.Long => value.longValue
    case _ => fail(s"expected a present long at $index")

  private def pairAt(values: Seq[(Long, Long)], index: Int): (Long, Long) =
    values.iterator.drop(index).next()

  /**
   * First row is a real pair so classification can witness long pairs. The second row throws if
   * the unused side is read. The `Iterable` cast does not touch the elements.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def sideSource(kept: Long, ordinal: Int): DistCollection[(Long, Long)] =
    val elements: Vector[AnyRef] =
      Vector(pair(longRef(4L), longRef(8L)), new OneSide(kept, ordinal))
    DistCollection(
      Plan.Source(
        Seq(Partition(elements.asInstanceOf[Iterable[(Long, Long)]])),
        Plan.SourceDescriptor.Unknown,
      ),
    )

  private def longComponent(row: (Long, Long), ordinal: Int): Long =
    component(row, ordinal) match
      case value: Long => value
      case _ => fail(s"expected a present long at ordinal $ordinal")

  /**
   * One side of a boxed pair. `productElement` throws for every ordinal other than `ordinal`, so
   * a keys or values encode that reads the unused side fails the test. The first row of the
   * source stays a real tuple: classification reads that row only.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Throw",
    ),
  )
  private final class OneSide(kept: Long, ordinal: Int) extends Product2[Long, Long]:

    def _1: Long = read(0).longValue

    def _2: Long = read(1).longValue

    def canEqual(that: Any): Boolean = false

    override def productArity: Int = 2

    override def productElement(n: Int): Any = read(n)

    private def read(n: Int): java.lang.Long =
      if (n == ordinal) then java.lang.Long.valueOf(kept)
      else throw new IllegalStateException(s"long-pair side $n was read")
