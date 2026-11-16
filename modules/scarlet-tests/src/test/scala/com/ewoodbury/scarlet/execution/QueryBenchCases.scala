package com.ewoodbury.scarlet.execution

import scala.util.Random

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{IntPairArrays, Plan, ScarletConf}

/**
 * Catalogue of [[QueryBench]] cases. Checksums live in the `Filter*Checksum` objects and the
 * element/sort checksum siblings. Grouped-repeat and grouped-shuffled construction stays here
 * because those folds pin first-seen order on a dense 1024-slot table.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object QueryBenchCases:

  val elementRows: Int = 1000000
  val groupedRows: Int = 10000000
  val groups: Int = 1024
  val sourcePartitions: Int = 2
  val batchSize: Int = 8192
  val lcgSeed: Int = 0x12345678
  val shuffledSeed: Long = 58L

  def buildNamed(name: String): Vector[QueryBench.Query] =
    name match
      case "filter-join" =>
        val generated = generate(elementRows)
        val filteredJoin = FilterJoinChecksum.source(generated, sourcePartitions)
        Vector(
          QueryBench.whole(
            "filter-join",
            elementRows,
            FilterJoinChecksum.shape,
            FilterJoinChecksum.expected(generated),
            filteredJoin.plan,
            FilterJoinChecksum.collected(filteredJoin.collect()),
          ),
        )
      case other =>
        val picked = build().filter(_.name == other)
        if (picked.isEmpty) then
          System.err.println(s"unknown query $other")
          sys.exit(1)
        else picked

  def build(): Vector[QueryBench.Query] =
    val generated = generate(elementRows)
    val narrowSum = IntNarrowChecksum.expected(generated)
    val chainSum = IntChainChecksum.expected(generated, steps = 4)
    val doubleSum = DoubleFilterChecksum.expected(generated)
    val ints = DistCollection.ofInts(generated, sourcePartitions)
    val narrow = ints.filter(_ > 0).map(_ * 2)
    val intChain = ints.map(_ + 1).map(_ + 1).map(_ + 1).map(_ + 1)
    val longs = DistCollection.ofLongs(generated.map(_.toLong), sourcePartitions)
    val longChain = longs.map(_ + 1L).map(_ + 1L).map(_ + 1L).map(_ + 1L)
    val doubles = DistCollection.ofDoubles(generated.map(_.toDouble), sourcePartitions)
    val positive = doubles.filter(_ > 0.0)
    val texts = StringEqualsChecksum.rows(elementRows)
    val strings = DistCollection.ofStrings(texts, sourcePartitions).filterStringEquals("k42")
    val flags = Array.tabulate(elementRows)(row => (row & 1) == 0)
    val bools = DistCollection.ofBools(flags, sourcePartitions).filter(identity)
    val sortKeys = permutation(elementRows)
    require(!isSorted(sortKeys), "sort keys stayed in order")
    val intSort = DistCollection.ofIntPairs(sortKeys, generated, sourcePartitions).sortByIntKey
    val longSortKeys = widen(sortKeys)
    val longSortValues = widen(generated)
    val longSort =
      DistCollection.ofLongPairs(longSortKeys, longSortValues, sourcePartitions).sortByLongKey
    val (repeating, shuffled, groupedTotal) = groupedSources()
    val repeatFold = sourceGroupFold(repeating)
    val shuffledFold = sourceGroupFold(shuffled)
    require(repeatFold.total == groupedTotal, "repeating groups drifted from the closed sum")
    require(shuffledFold.total == groupedTotal, "shuffled groups drifted from the closed sum")
    require(repeatFold.hash != shuffledFold.hash, "grouped checksums collided")
    val sparse = GroupedBenchCases.sparse(groupedRows, sourcePartitions, batchSize)
    val shifting = GroupedBenchCases.shifting(groupedRows, sourcePartitions, batchSize)
    require(sparse.hash != shifting.hash, "spread checksums collided")
    require(sparse.hash != repeatFold.hash, "sparse checksum collided with repeating groups")
    GroupedBenchCases.requireHashFallback(sparse, shifting, batchSize)
    val groupKeys = Array.tabulate(elementRows)(row => row % groups)
    val keptGroups = FilterGroupChecksum.expected(groupKeys, generated, groups)
    val filteredGroups = FilterGroupChecksum.source(groupKeys, generated, sourcePartitions)
    val keptCounts = FilterCountChecksum.expected(groupKeys, generated, groups)
    val filteredCounts = FilterCountChecksum.source(groupKeys, generated, sourcePartitions)
    val filteredCount = FilterReduceChecksum.countSource(groupKeys, generated, sourcePartitions)
    val stringGroups = FilterStringGroupChecksum.source(generated, sourcePartitions)
    val stringReduce = FilterStringReduceChecksum.source(generated, sourcePartitions)
    val keptReduce = FilterReduceChecksum.expected(groupKeys, generated, groups)
    val filteredReduce = FilterReduceChecksum.source(groupKeys, generated, sourcePartitions)
    val filteredJoin = FilterJoinChecksum.source(generated, sourcePartitions)
    val stringJoin = FilterStringJoinChecksum.source(generated, sourcePartitions)
    val filteredSort = FilterSortChecksum.source(sortKeys, generated, sourcePartitions)
    Vector(
      QueryBench.whole(
        "int-narrow",
        elementRows,
        IntNarrowChecksum.shape,
        narrowSum,
        narrow.plan,
        narrow.sum,
      ),
      QueryBench.whole(
        "int-chain",
        elementRows,
        IntChainChecksum.shape,
        chainSum,
        intChain.plan,
        intChain.sum,
      ),
      QueryBench.whole(
        "long-chain",
        elementRows,
        IntChainChecksum.shape,
        chainSum,
        longChain.plan,
        longChain.sum,
      ),
      QueryBench.real(
        "double",
        elementRows,
        DoubleFilterChecksum.shape,
        doubleSum,
        positive.plan,
        positive.sum,
      ),
      QueryBench.whole(
        "string",
        elementRows,
        StringEqualsChecksum.shape,
        StringEqualsChecksum.expected(elementRows),
        strings.plan,
        strings.count(),
      ),
      QueryBench.whole(
        "boolean",
        elementRows,
        BooleanFilterChecksum.shape,
        BooleanFilterChecksum.expected(elementRows),
        bools.plan,
        bools.count(),
      ),
      QueryBench.whole(
        "int-sort-key",
        elementRows,
        "SortByIntKey(Scan)",
        PairChecksum.expectedSortedInts(sortKeys, generated),
        intSort.plan,
        PairChecksum.intPairs(intSort.collect()),
      ),
      QueryBench.whole(
        "long-sort-key",
        elementRows,
        "SortByLongKey(Scan)",
        PairChecksum.expectedSortedLongs(longSortKeys, longSortValues),
        longSort.plan,
        PairChecksum.longPairs(longSort.collect()),
      ),
      grouped("grouped-repeat", repeating, repeatFold.hash),
      grouped("grouped-shuffled", shuffled, shuffledFold.hash),
      openGrouped("grouped-sparse", sparse),
      openGrouped("grouped-shifting", shifting),
      QueryBench.whole(
        "filter-groupbykey",
        elementRows,
        FilterGroupChecksum.shape,
        keptGroups,
        filteredGroups.plan,
        FilterGroupChecksum.collected(filteredGroups.collect(), groups),
      ),
      QueryBench.whole(
        "filter-countbykey",
        elementRows,
        FilterCountChecksum.shape,
        keptCounts,
        filteredCounts.plan,
        FilterCountChecksum.collected(filteredCounts.collect(), groups),
      ),
      QueryBench.whole(
        "filter-count",
        elementRows,
        FilterReduceChecksum.countShape,
        FilterReduceChecksum.expectedCount(groupKeys, generated),
        filteredCount.plan,
        filteredCount.count(),
      ),
      QueryBench.whole(
        "filter-string-groupbykey",
        elementRows,
        FilterStringGroupChecksum.shape,
        FilterStringGroupChecksum.expected(generated),
        stringGroups.plan,
        FilterStringGroupChecksum.collected(stringGroups.collect()),
      ),
      QueryBench.whole(
        "filter-reducebykey",
        elementRows,
        FilterReduceChecksum.shape,
        keptReduce,
        filteredReduce.plan,
        FilterReduceChecksum.collected(filteredReduce.collect(), groups),
      ),
      QueryBench.whole(
        "filter-string-reducebykey",
        elementRows,
        FilterStringReduceChecksum.shape,
        FilterStringReduceChecksum.expected(generated),
        stringReduce.plan,
        FilterStringReduceChecksum.collected(stringReduce.collect()),
      ),
      QueryBench.whole(
        "filter-join",
        elementRows,
        FilterJoinChecksum.shape,
        FilterJoinChecksum.expected(generated),
        filteredJoin.plan,
        FilterJoinChecksum.collected(filteredJoin.collect()),
      ),
      QueryBench.whole(
        "filter-string-join",
        elementRows,
        FilterStringJoinChecksum.shape,
        FilterStringJoinChecksum.expected(generated),
        stringJoin.plan,
        FilterStringJoinChecksum.collected(stringJoin.collect()),
      ),
      QueryBench.whole(
        "filter-sort",
        elementRows,
        FilterSortChecksum.shape,
        FilterSortChecksum.expected(sortKeys, generated),
        filteredSort.plan,
        FilterSortChecksum.collected(filteredSort.collect()),
      ),
    )

  private def grouped(
      name: String,
      source: DistCollection[(Int, Int)],
      hash: Long,
  ): QueryBench.Query =
    QueryBench.requireShape(name, "GroupedSum(Scan)", source.plan)
    new QueryBench.Query(
      name,
      groupedRows,
      "GroupedSum(Scan)",
      QueryBench.Answer.Whole(sourceGroupFold(source).total),
      () => QueryBench.Answer.Whole(source.groupedSum),
      Some(hash),
      () => sourceGroupFold(source),
      () => columnarGroupFold(source),
    )

  private def openGrouped(name: String, input: GroupedBenchCases.Input): QueryBench.Query =
    QueryBench.requireShape(name, "GroupedSum(Scan)", input.source.plan)
    val source = input.source
    new QueryBench.Query(
      name,
      groupedRows,
      "GroupedSum(Scan)",
      QueryBench.Answer.Whole(input.total),
      () => QueryBench.Answer.Whole(source.groupedSum),
      Some(input.hash),
      () => copied(GroupedBenchCases.sourceFold(source)),
      () => copied(GroupedBenchCases.columnarFold(source)),
    )

  private def copied(fold: GroupedBenchCases.Fold): QueryBench.GroupFold =
    QueryBench.GroupFold(fold.hash, fold.total)

  private def groupedSources(): (DistCollection[(Int, Int)], DistCollection[(Int, Int)], Long) =
    val keys = Array.tabulate(groupedRows)(row => row % groups)
    val values = Array.tabulate(groupedRows)(row => row)
    val shuffled = shuffleWithinMorsels(keys)
    val expected = groupedRows.toLong * (groupedRows.toLong - 1L) / 2L
    require(sumInts(values) == expected, "grouped values drifted from the closed sum")
    val repeating = DistCollection.ofIntPairs(keys, values, sourcePartitions)
    val permuted = DistCollection.ofIntPairs(shuffled, values, sourcePartitions)
    (repeating, permuted, expected)

  /**
   * Shuffles each morsel in place on a copy. Morsels are `columnarBatchSize` ranges from the start
   * of each source partition, which is how the columnar scan slices a partition. The key multiset
   * of each range stays equal to `row % groups`. Values stay in row order, so the grand total is
   * unchanged and the per-group sums are not.
   */
  private def shuffleWithinMorsels(keys: Array[Int]): Array[Int] =
    val copy = keys.clone()
    val random = new Random(shuffledSeed)
    val partRows = keys.length / sourcePartitions
    require(partRows * sourcePartitions == keys.length, "grouped rows do not split evenly")
    var part = 0
    while (part < sourcePartitions) {
      val origin = part * partRows
      var start = 0
      while (start < partRows) {
        val length = math.min(batchSize, partRows - start)
        fisherYates(copy, origin + start, length, random)
        require(
          sameCounts(keys, copy, origin + start, length),
          s"morsel multiset changed at ${origin + start}",
        )
        start += length
      }
      part += 1
    }
    require(!keys.sameElements(copy), "shuffled keys kept sequential order")
    copy

  private def fisherYates(values: Array[Int], start: Int, length: Int, random: Random): Unit =
    var offset = length - 1
    while (offset > 0) {
      val swap = random.nextInt(offset + 1)
      val left = start + offset
      val right = start + swap
      val held = values(left)
      values(left) = values(right)
      values(right) = held
      offset -= 1
    }

  private def sameCounts(left: Array[Int], right: Array[Int], start: Int, length: Int): Boolean =
    val counts = new Array[Int](groups)
    var index = 0
    while (index < length) {
      counts(left(start + index)) += 1
      index += 1
    }
    index = 0
    var matched = true
    while (index < length && matched) {
      val key = right(start + index)
      counts(key) -= 1
      if (counts(key) < 0) then matched = false
      index += 1
    }
    var cursor = 0
    while (cursor < counts.length && matched) {
      if (counts(cursor) != 0) then matched = false
      cursor += 1
    }
    matched

  /**
   * Folds pairs the row path reads. Present keys contribute `(key * 31) ^ groupSum`. The sum of
   * those terms is the checksum. Key order makes it independent of row order.
   */
  private def sourceGroupFold(source: DistCollection[(Int, Int)]): QueryBench.GroupFold =
    val sums = new Array[Long](groups)
    val seen = new Array[Int](groups)
    source.plan match
      case Plan.Source(partitions, _) =>
        partitions.foreach { partition =>
          partition.data match
            case pairs: IntPairArrays => addPairs(pairs.keys, pairs.values, sums, seen)
            case _ =>
              throw new IllegalStateException("grouped source was not an owned pair array")
        }
      case _ => throw new IllegalStateException("grouped plan was not a source")
    finishGroups(sums, seen)

  /**
   * Folds the groups the columnar kernel returns. Partials are already exchanged, and a key that
   * appears in more than one batch is accumulated before the fold.
   */
  private def columnarGroupFold(source: DistCollection[(Int, Int)]): QueryBench.GroupFold =
    require(
      ScarletConf.get.columnarExecution,
      "columnar group checksum ran with columnar execution off",
    )
    source.plan match
      case sourcePlan: Plan.Source[_] =>
        val encoded = SourceEncode.encodeGroupedPairs(sourcePlan.partitions)
        val summed = ColumnPipeline.sumByKey(
          encoded,
          keyOrdinal = 0,
          valueOrdinal = 1,
          numPartitions = ColumnarPlanner.shuffleWidth,
          parallelism = ColumnarPlanner.parallelism,
          batchSize = ColumnarPlanner.batchSize,
          steps = Vector.empty[PairStep],
        )
        val sums = new Array[Long](groups)
        val seen = new Array[Int](groups)
        summed.foreach(batch => addBatch(batch, sums, seen))
        finishGroups(sums, seen)
      case _ => throw new IllegalStateException("grouped plan was not a source")

  private def addPairs(
      keys: Array[Int],
      values: Array[Int],
      sums: Array[Long],
      seen: Array[Int],
  ): Unit =
    require(keys.length == values.length, "grouped key and value lengths differ")
    var index = 0
    while (index < keys.length) {
      addGroup(keys(index), values(index).toLong, sums, seen)
      index += 1
    }

  private def addBatch(batch: ColumnBatch, sums: Array[Long], seen: Array[Int]): Unit =
    if (batch.length != 0) then
      val keys = intKeys(batch)
      val values = longSums(batch)
      var row = 0
      while (row < batch.length) {
        require(keys.isValid(row) && values.isValid(row), "grouped checksum saw a null cell")
        addGroup(keys.values(row), values.values(row), sums, seen)
        row += 1
      }

  private def addGroup(key: Int, value: Long, sums: Array[Long], seen: Array[Int]): Unit =
    require(key >= 0 && key < groups, s"grouped checksum key $key outside 0 until $groups")
    sums(key) += value
    seen(key) += 1

  private def finishGroups(sums: Array[Long], seen: Array[Int]): QueryBench.GroupFold =
    var key = 0
    var hash = 0L
    var total = 0L
    var present = 0
    while (key < groups) {
      if (seen(key) > 0) then
        present += 1
        total += sums(key)
        hash += (key.toLong * 31L) ^ sums(key)
      key += 1
    }
    require(present == groups, s"grouped checksum saw $present groups, expected $groups")
    QueryBench.GroupFold(hash, total)

  private def intKeys(batch: ColumnBatch): Int32Column =
    batch.columns.headOption match
      case Some(column: Int32Column) => column
      case _ => throw new IllegalStateException("grouped key column was not int32")

  private def longSums(batch: ColumnBatch): Int64Column =
    batch.columns.lift(1) match
      case Some(column: Int64Column) => column
      case _ => throw new IllegalStateException("grouped sum column was not int64")

  private def generate(rows: Int): Array[Int] =
    val values = new Array[Int](rows)
    var index = 0
    var state = lcgSeed
    while (index < rows) {
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      values(index) = (mixed % 2000001) - 1000000
      index += 1
    }
    values

  private def permutation(rows: Int): Array[Int] =
    val keys = Array.tabulate(rows)(identity)
    fisherYates(keys, 0, rows, new Random(lcgSeed.toLong))
    keys

  private def isSorted(keys: Array[Int]): Boolean =
    var index = 1
    var ordered = true
    while (index < keys.length && ordered) {
      if (keys(index - 1) > keys(index)) then ordered = false
      index += 1
    }
    ordered

  private def widen(values: Array[Int]): Array[Long] =
    val widened = new Array[Long](values.length)
    var index = 0
    while (index < values.length) {
      widened(index) = values(index).toLong
      index += 1
    }
    widened

  private def sumInts(values: Array[Int]): Long =
    var index = 0
    var total = 0L
    while (index < values.length) {
      total += values(index).toLong
      index += 1
    }
    total
