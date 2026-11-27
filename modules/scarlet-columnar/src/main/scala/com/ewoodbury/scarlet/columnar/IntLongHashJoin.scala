package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Inner hash join of an `Int32` key and an `Int64` value on both sides.
 *
 * The table is the long shuffle-hash shape: open addressing on the key, flat key and value arrays,
 * and a chain so duplicate keys emit the full cartesian product. [[shuffled]] hash-partitions both
 * sides on the int key and probes each bucket. Output is `(key, left, right)`. Within a probe row
 * the build chain is newest-first. That walk is a kernel detail, not a public contract.
 *
 * A null key is not inserted and is not probed, so a stored `0` does not match key `0`. A clear
 * value bit is copied. A clear validity bit is a null long, not an absence bit, and the matched
 * row is kept. This kernel does not spill. Output columns keep the grown capacity and `length` is
 * the match count. This is not [[LongHashJoin]]: the key is an int, and the int hash join reads
 * its value as an int.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object IntLongHashJoin:

  def shuffled(
      build: Vector[ColumnBatch],
      probe: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"shuffle partitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (build.isEmpty || probe.isEmpty) then Vector.fill(numPartitions)(empty)
    else
      val buildBuckets =
        HashExchange.partitionAll(build, keyOrdinal = 0, numPartitions, parallelism)
      val probeBuckets =
        HashExchange.partitionAll(probe, keyOrdinal = 0, numPartitions, parallelism)
      MorselScheduler.map(probeBuckets, parallelism) { (part, probeBatch) =>
        inner(
          HashJoin.JoinSide(buildBuckets(part), key = 0, value = 1),
          HashJoin.JoinSide(probeBatch, key = 0, value = 1),
        )
      }

  def inner(build: HashJoin.JoinSide, probe: HashJoin.JoinSide): ColumnBatch =
    val buildKeys = ColumnHash.int32At(build.batch, build.key)
    val buildValues = ColumnHash.int64At(build.batch, build.value)
    val probeKeys = ColumnHash.int32At(probe.batch, probe.key)
    val probeValues = ColumnHash.int64At(probe.batch, probe.value)
    require(buildKeys.length == buildValues.length, "build key and value columns differ in length")
    require(probeKeys.length == probeValues.length, "probe key and value columns differ in length")
    if (buildKeys.length == 0 || probeKeys.length == 0) then empty
    else indexed(buildKeys, buildValues).write(probeKeys, probeValues)

  /**
   * `(Int, (Long, Long))` rows. A clear bit is a null reference, not zero. This is not
   * [[BatchCodec.decodeIntTriples]]: that decoder returns `(Int, (Int, Int))`.
   */
  def decode(batch: ColumnBatch, maxRows: Int): Seq[(Int, (Long, Long))] =
    val rows = decodedLength(batch, maxRows)
    val keys = ColumnKernel.asInt32(ColumnKernel.columnAt(batch, 0), 0)
    val left = ColumnKernel.asInt64(ColumnKernel.columnAt(batch, 1), 1)
    val right = ColumnKernel.asInt64(ColumnKernel.columnAt(batch, 2), 2)
    ColumnMetrics.decoded(rows)
    Seq.tabulate(rows)(row => erased(intRef(keys, row), longRef(left, row), longRef(right, row)))

  private def indexed(buildKeys: Int32Column, buildValues: Int64Column): BuildTable =
    val table = new BuildTable(buildKeys, buildValues)
    table.insert()
    table

  private final class BuildTable(buildKeys: Int32Column, buildValues: Int64Column):
    private val rows: Int = buildKeys.length
    private val slotCapacity: Int = ColumnHash.tableSize(rows)
    private val mask: Int = slotCapacity - 1
    private val slotKey: Array[Int] = new Array[Int](slotCapacity)
    private val head: Array[Int] = new Array[Int](slotCapacity)
    private val next: Array[Int] = new Array[Int](rows)
    private val buildKeyValues: Array[Int] = buildKeys.values
    private val keyWords: Array[Long] = buildKeys.validity.words

    Arrays.fill(head, -1)
    Arrays.fill(next, -1)

    def insert(): Unit =
      var row = 0
      while (row < rows) {
        // A null key stays out of the table. Its payload is 0, and inserting it would match
        // probe key 0.
        if (Validity.isSet(keyWords, row)) then
          val key = buildKeyValues(row)
          var slot = slotOf(key)
          var placed = false
          while (!placed) {
            if (head(slot) == -1) then
              head(slot) = row
              slotKey(slot) = key
              placed = true
            else if (slotKey(slot) == key) then
              next(row) = head(slot)
              head(slot) = row
              placed = true
            else slot = (slot + 1) & mask
          }
        row += 1
      }

    def write(probeKeys: Int32Column, probeValues: Int64Column): ColumnBatch =
      var outCapacity = math.max(16, probeKeys.length)
      var filled = 0
      var outKeys = new Array[Int](outCapacity)
      var outBuild = new Array[Long](outCapacity)
      var outProbe = new Array[Long](outCapacity)
      var buildWords = Validity.allocate(outCapacity)
      var probeWords = Validity.allocate(outCapacity)
      val buildValueValues = buildValues.values
      val buildValueWords = buildValues.validity.words
      val probeValueValues = probeValues.values
      val probeValueWords = probeValues.validity.words
      val probeKeyValues = probeKeys.values
      val probeKeyWords = probeKeys.validity.words
      val probeRows = probeKeys.length
      var row = 0
      while (row < probeRows) {
        if (Validity.isSet(probeKeyWords, row)) then
          val key = probeKeyValues(row)
          var slot = slotOf(key)
          var resolved = false
          while (!resolved) {
            if (head(slot) == -1) then resolved = true
            else if (slotKey(slot) == key) then
              var buildRow = head(slot)
              while (buildRow != -1) {
                if (filled == outCapacity) then
                  outCapacity = ColumnHash.doubled(outCapacity)
                  outKeys = Arrays.copyOf(outKeys, outCapacity)
                  outBuild = Arrays.copyOf(outBuild, outCapacity)
                  outProbe = Arrays.copyOf(outProbe, outCapacity)
                  buildWords = Validity.growWords(buildWords, outCapacity)
                  probeWords = Validity.growWords(probeWords, outCapacity)
                outKeys(filled) = buildKeyValues(buildRow)
                if (Validity.isSet(buildValueWords, buildRow)) then
                  outBuild(filled) = buildValueValues(buildRow)
                  Validity.setBit(buildWords, filled)
                if (Validity.isSet(probeValueWords, row)) then
                  outProbe(filled) = probeValueValues(row)
                  Validity.setBit(probeWords, filled)
                filled += 1
                buildRow = next(buildRow)
              }
              resolved = true
            else slot = (slot + 1) & mask
          }
        row += 1
      }
      if (filled == 0) then empty
      else publish(outKeys, outBuild, outProbe, buildWords, probeWords, outCapacity, filled)

    private def slotOf(key: Int): Int =
      ColumnHash.mix(java.lang.Integer.hashCode(key)) & mask

  private def publish(
      keys: Array[Int],
      buildValues: Array[Long],
      probeValues: Array[Long],
      buildWords: Array[Long],
      probeWords: Array[Long],
      capacity: Int,
      rows: Int,
  ): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int32Column.of(keys, Validity.prefixValid(capacity, rows), rows),
        Int64Column.of(buildValues, Validity.fromWords(buildWords, capacity), rows),
        Int64Column.of(probeValues, Validity.fromWords(probeWords, capacity), rows),
      ),
      rows,
    )

  private def empty: ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int32Column.of(Array.empty[Int], Validity.allValid(0), 0),
        Int64Column.of(Array.empty[Long], Validity.allValid(0), 0),
        Int64Column.of(Array.empty[Long], Validity.allValid(0), 0),
      ),
      0,
    )

  private def decodedLength(batch: ColumnBatch, maxRows: Int): Int =
    require(maxRows >= 0, s"maximum decoded rows must be >= 0, got $maxRows")
    math.min(batch.length, maxRows)

  /** A present cell is the stored int. A clear bit is a null reference, not `0`. */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def intRef(column: Int32Column, row: Int): java.lang.Integer =
    if (column.isValid(row)) java.lang.Integer.valueOf(column.values(row)) else null

  /** A present cell is the stored long. A clear bit is a null reference, not `0`. */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def longRef(column: Int64Column, row: Int): java.lang.Long =
    if (column.isValid(row)) java.lang.Long.valueOf(column.values(row)) else null

  /**
   * Named erasure boundary. The decoded triple has to carry a null long. Building it from
   * `java.lang.Integer` and `java.lang.Long` keeps a null from becoming zero.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def erased(
      key: java.lang.Integer,
      left: java.lang.Long,
      right: java.lang.Long,
  ): (Int, (Long, Long)) =
    Tuple2(key, Tuple2(left, right)).asInstanceOf[(Int, (Long, Long))]
