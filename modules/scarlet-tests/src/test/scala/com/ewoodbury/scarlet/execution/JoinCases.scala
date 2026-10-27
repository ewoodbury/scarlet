package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.Plan

/**
 * Join inputs for [[JoinBench]]. Each case is a [[DistCollection]] join. `expectedPairs` is the
 * pair count implied by the generator, and the bench rejects a result that disagrees with it.
 * Outer cases reuse the symmetric and duplicate generators. The row executor shuffle-hashes every
 * outer join and ignores the hint. The columnar planner follows that hint.
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
  // 200k rows and 1k keys is a 40M-row product. The outer oracle keeps every row, and each
  // matched row carries an Option. That multiset does not fit the 3GB sbt heap. 10k keys on
  // the same 200k rows leaves 20 copies per key, so the product is still the cost.
  val outerDuplicateKeys: Int = 10000
  val hotRows: Int = 100000

  final case class JoinSpec(
      name: String,
      leftRows: Int,
      rightRows: Int,
      threshold: Long,
      sortMerge: Boolean,
      plan: Plan[_],
      expectedPairs: Long,
      outer: Boolean,
  )

  def full: Vector[JoinSpec] = Vector(
    broadcast(probeRows, buildRows),
    symmetric(symmetricRows),
    duplicates(duplicateRows, distinctKeys),
    hotKey(hotRows),
  )

  /**
   * Left, right, and full outer joins on the symmetric inputs, plus a duplicate-key left outer
   * join. Scales match [[full]].
   */
  def outer: Vector[JoinSpec] = Vector(
    leftOuter(symmetricRows),
    rightOuter(symmetricRows),
    fullOuter(symmetricRows),
    outerDuplicates(duplicateRows, outerDuplicateKeys),
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
      probe.broadcastJoin(build).plan,
      expectedPairs = probeRows.toLong,
    )

  /** Both sides use unique keys. The right side is shifted by half, so half the keys match. */
  def symmetric(rows: Int): JoinSpec =
    require(rows > 0 && rows % 2 == 0, s"symmetric rows must be a positive even count, got $rows")
    val (left, right) = shifted(rows)
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

  /**
   * Same keys as [[symmetric]]. Half of the right keys sit past the left range, so half of the
   * probe rows have no partner. Every left row is kept. Hint is absent, which both paths run as
   * shuffle-hash.
   */
  def leftOuter(rows: Int): JoinSpec =
    require(rows > 0 && rows % 2 == 0, s"left-outer rows must be a positive even count, got $rows")
    val (left, right) = shifted(rows)
    outerSpec(
      "left-outer",
      rows,
      Plan.LeftOuterJoinOp(left.plan, right.plan, None),
      expectedPairs = rows.toLong,
    )

  /**
   * The mirror of [[leftOuter]]: the same inputs, preserving the right side. The hint is
   * broadcast. The row path still shuffle-hashes. The columnar path builds the left input.
   */
  def rightOuter(rows: Int): JoinSpec =
    require(
      rows > 0 && rows % 2 == 0,
      s"right-outer rows must be a positive even count, got $rows",
    )
    val (left, right) = shifted(rows)
    outerSpec(
      "right-outer",
      rows,
      Plan.RightOuterJoinOp(left.plan, right.plan, Some(Plan.JoinStrategy.Broadcast)),
      expectedPairs = rows.toLong,
    )

  /**
   * The same inputs again. Each side keeps the keys the other side does not have, so the result
   * is larger than either input. The hint is shuffle-hash on both paths.
   */
  def fullOuter(rows: Int): JoinSpec =
    require(rows > 0 && rows % 2 == 0, s"full-outer rows must be a positive even count, got $rows")
    val (left, right) = shifted(rows)
    outerSpec(
      "full-outer",
      rows,
      Plan.FullOuterJoinOp(left.plan, right.plan, Some(Plan.JoinStrategy.ShuffleHash)),
      expectedPairs = 3L * rows.toLong / 2L,
    )

  /**
   * Same row count as [[duplicates]], with more keys so the product fits in the sbt heap. Every
   * key is on both sides, so the result is the cartesian product and there is no missing side.
   * The product is the cost. The hint is absent.
   */
  def outerDuplicates(rows: Int, distinct: Int): JoinSpec =
    require(distinct > 0 && rows > 0 && rows % distinct == 0, "duplicate rows must divide evenly")
    val perKey = rows / distinct
    val left = intPairs(rows, row => row % distinct, row => row)
    val right = intPairs(rows, row => row % distinct, row => row)
    val pairs = distinct.toLong * perKey.toLong * perKey.toLong
    outerSpec(
      "outer-duplicates",
      rows,
      Plan.LeftOuterJoinOp(left.plan, right.plan, None),
      pairs,
    )

  /**
   * A row whose right value is null, not `None`. The static type hides that null the same way
   * [[joinRow]] hides a null key. Digest tests use this.
   */
  def nullValueRow(key: Int, left: Int): (Int, (Int, Int)) =
    Tuple2(Int.box(key), Tuple2(Int.box(left), null)).asInstanceOf[(Int, (Int, Int))]

  /** A join row with a possibly null key, for digest tests. Values stay present. */
  def joinRow(key: Option[Int], left: Int, right: Int): (Int, (Int, Int)) =
    val boxedKey: AnyRef = key match
      case Some(present) => Int.box(present)
      case None => null
    val values = Tuple2(Int.box(left), Int.box(right)).asInstanceOf[(Int, Int)]
    Tuple2(boxedKey, values).asInstanceOf[(Int, (Int, Int))]

  def hint(spec: JoinSpec): Option[Plan.JoinStrategy] = spec.plan match
    case Plan.JoinOp(_, _, strategy) => strategy
    case Plan.LeftOuterJoinOp(_, _, strategy) => strategy
    case Plan.RightOuterJoinOp(_, _, strategy) => strategy
    case Plan.FullOuterJoinOp(_, _, strategy) => strategy
    case _ => throw new IllegalStateException(s"${spec.name} is not a join")

  /** `unhinted` when the plan stores no strategy. Otherwise the hint's name. */
  def hintLabel(spec: JoinSpec): String = hint(spec) match
    case None => "unhinted"
    case Some(strategy) => strategyName(strategy)

  def strategyName(strategy: Plan.JoinStrategy): String = strategy match
    case Plan.JoinStrategy.Broadcast => "broadcast"
    case Plan.JoinStrategy.ShuffleHash => "shuffle-hash"
    case Plan.JoinStrategy.SortMerge => "sort-merge"

  /**
   * Path the row executor runs. An outer join is always shuffle-hash: the executor ignores the
   * hint. An inner join uses the hint when the plan has one. Otherwise the size rule in
   * `JoinExecutor.selectJoinStrategy` applied to these row counts and this case's threshold and
   * sort-merge flag.
   */
  def rowStrategy(spec: JoinSpec): String =
    if (spec.outer) then "shuffle-hash"
    else
      hint(spec) match
        case Some(strategy) => strategyName(strategy)
        case None =>
          val left = spec.leftRows.toLong
          val right = spec.rightRows.toLong
          if (left <= spec.threshold || right <= spec.threshold) then "broadcast"
          else if (spec.sortMerge) then "sort-merge"
          else "shuffle-hash"

  /**
   * Runs `spec`. The cast is the erasure boundary: [[JoinSpec.plan]] has no element type, and
   * `count` does not use one. The plan is the join these cases build.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def count(spec: JoinSpec): Long =
    val collection: DistCollection[Any] =
      DistCollection(spec.plan.asInstanceOf[Plan[Any]])
    collection.count()

  /**
   * One result digest. Outer rows go through [[JoinDigest.fromOuter]] so a missing side stays
   * `None`. Inner rows stay on [[JoinDigest.from]], which rejects a null value.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def digest(spec: JoinSpec): JoinDigest.Digest =
    val collection: DistCollection[Any] =
      DistCollection(spec.plan.asInstanceOf[Plan[Any]])
    val rows: Iterable[Any] = collection.collect()
    if (spec.outer) then JoinDigest.fromOuter(rows)
    else JoinDigest.from(rows.asInstanceOf[Iterable[(Int, (Int, Int))]])

  private def shifted(rows: Int): (DistCollection[(Int, Int)], DistCollection[(Int, Int)]) =
    val left = intPairs(rows, row => row, row => row)
    val right = intPairs(rows, row => row + rows / 2, row => row)
    (left, right)

  private def forcedHash(
      name: String,
      leftRows: Int,
      rightRows: Int,
      joined: DistCollection[(Int, (Int, Int))],
      expectedPairs: Long,
  ): JoinSpec =
    spec(name, leftRows, rightRows, threshold = 0L, sortMerge = false, joined.plan, expectedPairs)

  private def outerSpec(
      name: String,
      rows: Int,
      plan: Plan[_],
      expectedPairs: Long,
  ): JoinSpec =
    JoinSpec(
      name,
      rows,
      rows,
      threshold = 0L,
      sortMerge = false,
      plan,
      expectedPairs,
      outer = true,
    )

  private def spec(
      name: String,
      leftRows: Int,
      rightRows: Int,
      threshold: Long,
      sortMerge: Boolean,
      plan: Plan[_],
      expectedPairs: Long,
  ): JoinSpec =
    JoinSpec(name, leftRows, rightRows, threshold, sortMerge, plan, expectedPairs, outer = false)

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


