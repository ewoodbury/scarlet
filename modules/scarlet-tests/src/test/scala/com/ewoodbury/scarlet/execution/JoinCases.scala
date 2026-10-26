package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.Plan

/**
 * Join inputs for [[JoinBench]]. Each case is a [[DistCollection]] join. `expectedPairs` is the
 * pair count implied by the generator, and the bench rejects a result that disagrees with it.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Var",
  ),
)
private[execution] object JoinCases:

  val sourcePartitions: Int = 2
  val probeRows: Int = 1000000
  val buildRows: Int = 1000
  // 1M per side on the row path is allocation-bound and dominated by GC in the sbt JVM, so it
  // measures the collector rather than the join. 300k keeps the shape measurable.
  val symmetricRows: Int = 300000
  val duplicateRows: Int = 200000
  val distinctKeys: Int = 1000
  val nullRows: Int = 100000
  val hotRows: Int = 100000

  final case class JoinSpec(
      name: String,
      leftRows: Int,
      rightRows: Int,
      threshold: Long,
      sortMerge: Boolean,
      joined: DistCollection[(Int, (Int, Int))],
      expectedPairs: Long,
  )

  def full: Vector[JoinSpec] = Vector(
    broadcast(probeRows, buildRows),
    symmetric(symmetricRows),
    duplicates(duplicateRows, distinctKeys),
    hotKey(hotRows),
  )

  /**
   * Build keys are `0` until `buildRows`, one row each. Probe keys are `row % buildRows`, so
   * every probe row matches exactly one build row. The build side is the small side.
   */
  def broadcast(probeRows: Int, buildRows: Int): JoinSpec =
    require(probeRows > 0 && buildRows > 0, "broadcast sides must be non-empty")
    val build = intPairs(buildRows, row => row, row => row)
    val probe = intPairs(probeRows, row => row % buildRows, row => row)
    spec(
      "broadcast-shaped",
      probeRows,
      buildRows,
      threshold = buildRows.toLong,
      sortMerge = true,
      probe.broadcastJoin(build),
      expectedPairs = probeRows.toLong,
    )

  /** Both sides use unique keys. The right side is shifted by half, so half the keys match. */
  def symmetric(rows: Int): JoinSpec =
    require(rows > 0 && rows % 2 == 0, s"symmetric rows must be a positive even count, got $rows")
    val left = intPairs(rows, row => row, row => row)
    val right = intPairs(rows, row => row + rows / 2, row => row)
    forcedHash("symmetric", rows, rows, left.join(right), expectedPairs = rows.toLong / 2L)

  /**
   * `distinct` keys, the same multiset on each side. Each key's output is the product of its two
   * side counts, which is larger than either input when both sides repeat every key.
   */
  def duplicates(rows: Int, distinct: Int): JoinSpec =
    require(distinct > 0 && rows > 0 && rows % distinct == 0, "duplicate rows must divide evenly")
    val perKey = rows / distinct
    val left = intPairs(rows, row => row % distinct, row => row)
    val right = intPairs(rows, row => row % distinct, row => row)
    val pairs = distinct.toLong * perKey.toLong * perKey.toLong
    forcedHash("duplicates", rows, rows, left.join(right), pairs)

  /**
   * A fixed small group shares key `0`; every other row keeps a unique key that exists on both
   * sides. The group size is deliberately small: a large hot key multiplies the output
   * quadratically, which measures the cartesian product rather than the join.
   */
  def hotKey(rows: Int): JoinSpec =
    require(rows > 0 && rows % 2 == 0, s"hot-key rows must be a positive even count, got $rows")
    val hotCount = math.max(1, math.min(2000, rows / 2))
    val keyOf: Int => Int = row => if row < hotCount then 0 else row
    val left = intPairs(rows, keyOf, row => row)
    val right = intPairs(rows, keyOf, row => row)
    val unique = rows.toLong - hotCount
    forcedHash("hot-key", rows, rows, left.join(right), hotCount.toLong * hotCount + unique)

  /** A join row with a possibly null key, for digest tests. Values stay present. */
  def joinRow(key: Option[Int], left: Int, right: Int): (Int, (Int, Int)) =
    val boxedKey: AnyRef = key match
      case Some(present) => Int.box(present)
      case None => null
    val values = Tuple2(Int.box(left), Int.box(right)).asInstanceOf[(Int, Int)]
    Tuple2(boxedKey, values).asInstanceOf[(Int, (Int, Int))]

  def hint(spec: JoinSpec): Option[Plan.JoinStrategy] = spec.joined.plan match
    case Plan.JoinOp(_, _, strategy) => strategy
    case _ => throw new IllegalStateException(s"${spec.name} is not a join")

  def strategyName(strategy: Plan.JoinStrategy): String = strategy match
    case Plan.JoinStrategy.Broadcast => "broadcast"
    case Plan.JoinStrategy.ShuffleHash => "shuffle-hash"
    case Plan.JoinStrategy.SortMerge => "sort-merge"

  /**
   * Hint when the plan has one. Otherwise the size rule in `JoinExecutor.selectJoinStrategy`
   * applied to these row counts and this case's threshold and sort-merge flag.
   */
  def rowStrategy(spec: JoinSpec): String = hint(spec) match
    case Some(strategy) => strategyName(strategy)
    case None =>
      val left = spec.leftRows.toLong
      val right = spec.rightRows.toLong
      if (left <= spec.threshold || right <= spec.threshold) then "broadcast"
      else if (spec.sortMerge) then "sort-merge"
      else "shuffle-hash"

  private def forcedHash(
      name: String,
      leftRows: Int,
      rightRows: Int,
      joined: DistCollection[(Int, (Int, Int))],
      expectedPairs: Long,
  ): JoinSpec =
    spec(name, leftRows, rightRows, threshold = 0L, sortMerge = false, joined, expectedPairs)

  private def spec(
      name: String,
      leftRows: Int,
      rightRows: Int,
      threshold: Long,
      sortMerge: Boolean,
      joined: DistCollection[(Int, (Int, Int))],
      expectedPairs: Long,
  ): JoinSpec =
    JoinSpec(name, leftRows, rightRows, threshold, sortMerge, joined, expectedPairs)

  private def intPairs(
      rows: Int,
      keyOf: Int => Int,
      valueOf: Int => Int,
  ): DistCollection[(Int, Int)] =
    val keys = new Array[Int](rows)
    val values = new Array[Int](rows)
    var index = 0
    while (index < rows) {
      keys(index) = keyOf(index)
      values(index) = valueOf(index)
      index += 1
    }
    DistCollection.ofIntPairs(keys, values, sourcePartitions)


