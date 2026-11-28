package com.ewoodbury.scarlet.execution
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/** Checks shared by [[TestCountByKey]]. */
trait CountByKeySupport extends AnyFlatSpec with Matchers:
  protected def assertFolded(
      filtered: DistCollection[(Int, Int)],
      expected: Map[Int, Int],
  ): Unit =
    val counted = filtered.countByKey
    foldsOf(counted.plan) shouldBe true
    FoldedCount.resetPassesForTest()
    val found = counted.collect().toList
    FoldedCount.passesForTest shouldBe 1
    found shouldBe hashCounted(counted)
    countsOf(found) shouldBe expected
    countsOf(found) shouldBe countsOf(row(counted.collect()))

  protected def foldsOf(plan: Plan[_]): Boolean =
    ColumnarPlanner.lower(plan) match
      case Some(op) =>
        (plan, op) match
          case (
                Plan.CountByKeyOp(source),
                PhysicalOp.CountByKey(PhysicalOp.Exchange(child)),
              ) =>
            CountByKeyArms.folds(source, child, ColumnUse.ordinals(plan, op))
          case _ => false
      case None => false

  protected def hashCounted(counted: DistCollection[(Int, Int)]): List[(Int, Int)] =
    val plan = counted.plan
    val op = ColumnarPlanner.lower(plan).getOrElse(fail("countByKey did not lower"))
    val (source, child) = (plan, op) match
      case (
            Plan.CountByKeyOp(source),
            PhysicalOp.CountByKey(PhysicalOp.Exchange(child)),
          ) =>
        (source, child)
      case _ => fail(s"countByKey lowered to ${op.shape}")
    val ordinals = ColumnUse.ordinals(plan, op)
    val selected = FilterChain
      .selected(source, child, None, preserveAbsence = false, ordinals)
      .getOrElse(fail("filter stack did not select"))
    val conf = ScarletConf.get
    decoded(
      HashCount.byKeySelected(
        selected,
        conf.defaultShufflePartitions,
        conf.threadPoolSize,
        conf.columnarBatchSize,
      ),
    )

  protected def assertSameOrder(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      partitions: Int,
      batchSize: Int,
  ): Unit =
    val viaFold = folded(batches, steps, partitions, batchSize)
    val selected = batches.map(batch => assembled(batch, steps, batchSize))
    val viaHash = HashCount.byKeySelected(selected, partitions, parallelism = 2, batchSize)
    decoded(viaFold) shouldBe decoded(viaHash)

  protected def folded(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      partitions: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    FoldedCount
      .count(batches, steps, partitions, parallelism = 2, batchSize)
      .getOrElse(fail("folded count declined"))

  protected def assembled(
      batch: ColumnBatch,
      steps: List[Selection.Step],
      batchSize: Int,
  ): Selected =
    val morsels = Morsels.of(batch, batchSize).map { morsel =>
      Selection.applySelected(morsel.batch, morsel.start, morsel.length, steps)
    }
    val rows = new Array[Int](batch.length)
    val total = morsels.foldLeft(0) { (written, morsel) =>
      System.arraycopy(morsel.rows, 0, rows, written, morsel.count)
      written + morsel.count
    }
    Selected(batch, rows, total)

  protected def decoded(batches: Vector[ColumnBatch]): List[(Int, Int)] =
    batches.flatMap(batch => BatchCodec.decodeIntPairs(batch)).toList

  /**
   * The `QueryBench` generator. The pinned checksum is `(key * 31) xor count`, folded in key
   * order.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  protected def benchValues(rows: Int): Array[Int] =
    val values = new Array[Int](rows)
    var index = 0
    var state = 0x12345678
    while (index < rows) {
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      values(index) = (mixed % 2000001) - 1000000
      index += 1
    }
    values

  protected def nonNegative(value: Any): Boolean = value match
    case found: Int => found >= 0
    case _ => false

  /**
   * Named erasure boundary. A present pair witnesses the column, and a later null key stays
   * absent. `countByKey` drops that key.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Null",
    ),
  )
  protected def nullKeySource(): DistCollection[(Any, Any)] =
    def cell(key: Any, value: Any): (Any, Any) = (key, value)
    DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq(cell(1, 4), cell(null, 9), cell(1, 5), cell(2, -1))),
          Partition(Seq(cell(2, 6), cell(null, 8))),
        ),
      ),
    )

  /**
   * A present pair witnesses the column. Later null keys stay absent and are dropped. The value
   * filter keeps non-negative ints.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  protected def nullKeyCounts(): Map[Int, Int] =
    val nullable = nullKeySource().filterValues(nonNegative).countByKey
    val found = anyCounts(nullable.collect())
    found shouldBe anyCounts(row(nullable.collect()))
    found

  /** Named erasure boundary. The boxed key is an `Int` once the null keys have been dropped. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Any"))
  protected def anyCounts(rows: Iterable[(Any, Int)]): Map[Int, Int] =
    countsOf(rows.map { case (key, count) => (key.asInstanceOf[Int], count) })

  protected def assertInt(source: DistCollection[(Int, Int)], expected: Map[Int, Int]): Unit =
    val counted = source.countByKey
    assertColumnar(counted.plan, "CountByKey(Exchange(Scan))")
    val found = countsOf(counted.collect())
    found shouldBe countsOf(row(counted.collect()))
    found shouldBe expected
    counted.count() shouldBe expected.size.toLong

  protected def assertString(
      source: DistCollection[(String, Int)],
      expected: Map[String, Int],
  ): Unit =
    val counted = source.countByKey
    val lowered = ColumnarPlanner.lower(counted.plan)
    lowered.map(_.shape) shouldBe Some("CountByKey(Exchange(Scan))")
    lowered.map(_.kind) shouldBe Some(PhysicalOp.Kind.StringPairs)
    ColumnarPlanner.partitioning(counted.plan).map(_.layout) shouldBe Some(
      Layout.StringPair(ScarletConf.get.columnarBatchSize),
    )
    val found = countsOf(counted.collect())
    found shouldBe countsOf(row(counted.collect()))
    found shouldBe expected

  protected def assertLong(source: DistCollection[(Long, Long)], expected: Map[Long, Int]): Unit =
    val counted = source.countByKey
    val lowered = ColumnarPlanner.lower(counted.plan)
    lowered.map(_.shape) shouldBe Some("CountByKey(Exchange(Scan))")
    lowered.map(_.kind) shouldBe Some(PhysicalOp.Kind.LongCounts)
    ColumnarPlanner.partitioning(counted.plan).map(_.layout) shouldBe Some(
      Layout.Columnar(ScarletConf.get.columnarBatchSize),
    )
    val found = counted.collect().toList
    found should contain theSameElementsAs row(counted.collect().toList)
    countsOf(found) shouldBe expected
    countsOf(found) shouldBe countsOf(row(counted.collect()))

  protected def assertColumnar(plan: Plan[_], shape: String): Unit =
    ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(shape)
    ColumnarPlanner.partitioning(plan).map(_.layout) shouldBe Some(
      Layout.Columnar(ScarletConf.get.columnarBatchSize),
    )

  protected def countsOf[K](rows: Iterable[(K, Int)]): Map[K, Int] =
    rows.foldLeft(Map.empty[K, Int]) { (seen, row) =>
      require(!seen.contains(row._1), s"duplicate count for ${row._1}")
      seen.updated(row._1, row._2)
    }

  protected def kernelCounts(batches: Vector[ColumnBatch]): Map[Int, Int] =
    countsOf(batches.flatMap(batch => BatchCodec.decodeIntPairs(batch)))

  protected def intPairs(
      keys: Array[Int],
      values: Array[Int],
      keyPresent: Int => Boolean = _ => true,
      valuePresent: Int => Boolean = _ => true,
  ): ColumnBatch =
    val rows = keys.length
    require(values.length == rows, "key and value columns differ in length")
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keys, Validity.pack(rows, keyPresent), rows),
        Int32Column.of(values, Validity.pack(rows, valuePresent), rows),
      ),
      rows,
    )

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  protected def lateNullSource(): DistCollection[(Any, Any)] =
    DistCollection(
      Plan.Source(Seq(Partition(Seq[(Any, Any)]((1, 2), (1, null), (2, 3))))),
    )

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
  protected def lateNullCounts(): Map[Int, Int] =
    require(lateNullKept(), "late-null source unboxed the null value")
    val counted = lateNullSource().countByKey
    require(ColumnarPlanner.lower(counted.plan).nonEmpty, "late-null source did not lower")
    require(ColumnarPlanner.tryRun(counted.plan).nonEmpty, "late-null count left the column path")
    counted.collect().map { case (key, value) => (key.asInstanceOf[Int], value) }.toMap

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Equals",
    ),
  )
  protected def lateNullOneFilterCounts(): Map[Int, Int] =
    val counted = lateNullSource().filterKeys { (key: Any) =>
      key match
        case found: Int => found == 1
        case _ => false
    }.countByKey
    require(
      ColumnarPlanner.lower(counted.plan).nonEmpty,
      "one-filter late-null source did not lower",
    )
    require(
      ColumnarPlanner.tryRun(counted.plan).nonEmpty,
      "one-filter late-null count left the column path",
    )
    counted.collect().map { case (key, value) => (key.asInstanceOf[Int], value) }.toMap

  protected def lateNullKept(): Boolean =
    val plan = lateNullSource().plan
    val op = ColumnarPlanner.lower(plan).getOrElse(fail("late-null source did not lower"))
    ColumnarInterpreter.pairsOf(ColumnarInterpreter.interpret(plan, op)).exists { batch =>
      val values = batch.columns.lift(1) match
        case Some(column: Int32Column) => column
        case other => fail(s"expected an Int32 value column, got $other")
      (0 until batch.length).exists(row => !values.isValid(row) && values.values(row) == 0)
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
  protected def lateNullRowCounts(): Map[Int, Int] =
    lateNullSource().countByKey
      .collect()
      .map { case (key, value) =>
        (key.asInstanceOf[Int], value)
      }
      .toMap

  /** Named erasure boundary. `Tuple2` keeps a null `Integer`; `(String, Int)` would unbox it. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  protected def stringPair(key: String, value: java.lang.Integer): (String, Int) =
    Tuple2(key, value).asInstanceOf[(String, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  protected def absentInt: java.lang.Integer = null

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  protected def longPair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  protected def absent: java.lang.Long = null

  protected def longRef(value: Long): java.lang.Long = java.lang.Long.valueOf(value)

  private type LongPairMap = ((Long, Long)) => (Long, Long)

  /**
   * A null key becomes `nullKey`. A present key is incremented. `productElement` keeps a null
   * value, so both paths count it. The row path and the long-pair project call this once per row.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Throw",
    ),
  )
  protected def shiftNullLongKey(calls: AtomicInteger, nullKey: Long): LongPairMap =
    pair => {
      val _ = calls.incrementAndGet()
      val product = pair.asInstanceOf[Product]
      val value = product.productElement(1)
      val key = product.productElement(0) match
        case null => nullKey
        case present: Long => present + 1L
        case other => throw new IllegalArgumentException(s"pair map saw $other")
      Tuple2(Long.box(key), value).asInstanceOf[(Long, Long)]
    }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  protected def nullLongKeySource(): DistCollection[(Long, Long)] =
    def cell(key: Any, value: Long): (Long, Long) =
      Tuple2(key, Long.box(value)).asInstanceOf[(Long, Long)]
    val first = Seq(
      cell(longRef(1L), 4L),
      cell(null, 9L),
      cell(longRef(1L), 5L),
      cell(longRef(2L), -1L),
    )
    val second = Seq(cell(longRef(2L), 6L), cell(null, 8L))
    DistCollection(Plan.Source(Seq(Partition(first), Partition(second))))

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  protected def nullLongValueSource(): DistCollection[(Long, Long)] =
    def cell(key: Long, value: Any): (Long, Long) =
      Tuple2(Long.box(key), value).asInstanceOf[(Long, Long)]
    DistCollection(Plan.Source(Seq(Partition(Seq(cell(4L, longRef(4L)), cell(1L, null))))))

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  protected def nullKey(value: Int): (String, Int) = (null: String) -> value

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  protected def nullValue(key: Int): (Int, String) = key -> (null: String)

  /**
   * A null key becomes `nullKey`. A present key is incremented. `productElement` keeps a null
   * value, so both paths count it. The row path and the pair project call this once per row.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Throw",
    ),
  )
  protected def shiftNullKey(calls: AtomicInteger, nullKey: Int): ((Int, Int)) => (Int, Int) =
    pair => {
      val _ = calls.incrementAndGet()
      val product = pair.asInstanceOf[Product]
      val value = product.productElement(1)
      val key = product.productElement(0) match
        case null => nullKey
        case present: Int => present + 1
        case other => throw new IllegalArgumentException(s"pair map saw $other")
      Tuple2(Int.box(key), value).asInstanceOf[(Int, Int)]
    }

  /** A present pair witnesses the scan, then a null value. The key stays present. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  protected def nullIntValueSource(): DistCollection[(Int, Int)] =
    def cell(key: Int, value: Any): (Int, Int) =
      Tuple2(Int.box(key), value).asInstanceOf[(Int, Int)]
    DistCollection(Plan.Source(Seq(Partition(Seq(cell(4, 4), cell(1, null))))))

  /**
   * A present pair witnesses the column. A later null key stays absent and `countByKey` drops it.
   * The value is a present int, so `mapValues` can declare `Int => Int`.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  protected def nullIntKeySource(): DistCollection[(Int, Int)] =
    def cell(key: Any, value: Int): (Int, Int) =
      Tuple2(key, Int.box(value)).asInstanceOf[(Int, Int)]
    DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq(cell(1, 4), cell(null, 9), cell(1, 5), cell(2, -1))),
          Partition(Seq(cell(2, 6), cell(null, 8))),
        ),
      ),
    )

  /** Low 14 bits of [[com.ewoodbury.scarlet.columnar.ColumnHash.mix]] for a 16384-slot table. */
  protected def mixSlot(key: Int): Int =
    val first = (key ^ (key >>> 16)) * 0x7feb352d
    val second = (first ^ (first >>> 15)) * 0x846ca68b
    (second ^ (second >>> 16)) & 16383

  protected def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
