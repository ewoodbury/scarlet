package com.ewoodbury.scarlet.columnar

/**
 * Hash-partitions batches on an `Int32`, `Int64`, or dictionary string key.
 *
 * The int bucket is `floorMod(key, numPartitions)`, the same rule `HashPartitioner` uses for an
 * `Int` key, because `hashCode` is the value. The long bucket is
 * `floorMod(Long.hashCode(key), numPartitions)`, the same rule for a `Long` key. The string bucket
 * is `floorMod(StringHash.hash(text), numPartitions)`. Each dictionary string is hashed once. A
 * different mix would send one key to different partitions on the two paths. Null keys are dropped
 * unless `carryNullKeys` is set. A carried null key goes to partition 0, the bucket
 * `HashPartitioner` uses for a null key. The default is off, so an inner join, the build side of
 * an outer join, and [[StringHashReduce]] still drop a null key. `""` hashes to 0 and is kept
 * either way. Null values stay null and move with the row. Order inside a bucket is input order.
 * An empty bucket is an empty batch of the same schema. An int bucket can be split again by
 * [[partitionInt32Within]] with `floorMod(floorDiv(key, parentWidth), parts)`, so keys that share
 * a bucket can still separate. String buckets keep the source dictionary; [[ColumnBatches.concat]]
 * remaps unlike dictionaries when those buckets are combined, so code equality in the combined
 * bucket is string equality.
 *
 * [[partitionAllSelected]] scatters an int-pair [[Selected]]. It reads `values(rows(i))` for
 * `i < count` and does not share the source, including when `numPartitions` is 1. A null key is
 * dropped. A null value stays null and moves with the row. [[partitionAllSelectedUtf8]] is that
 * scatter for a dictionary string key. It reads `rows(0 until count)`, drops a null key, keeps
 * `""`, and shares the source dictionary. `numPartitions == 1` still scatters when the selection
 * is not the whole batch. The dense loops are unchanged.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object HashExchange:

  /** Partition a null key lands in when `carryNullKeys` is set. Matches `HashPartitioner`. */
  private val nullKeyPartition = 0

  def partitionInt32(
      batch: ColumnBatch,
      keyOrdinal: Int,
      numPartitions: Int,
      carryNullKeys: Boolean = false,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val rows = batch.length
    // One partition already holds every row. Sharing the batch is safe when null keys are kept,
    // and when there is no null key to drop.
    if (numPartitions == 1 && (carryNullKeys || ColumnHash.fullyPresent(keys))) then Vector(batch)
    else
      val dest = new Array[Int](rows)
      val counts = new Array[Int](numPartitions)
      val keyValues = keys.values
      val keyWords = keys.validity.words
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then
          val part = Math.floorMod(keyValues(row), numPartitions)
          dest(row) = part
          counts(part) += 1
        else if (carryNullKeys) then
          dest(row) = nullKeyPartition
          counts(nullKeyPartition) += 1
        else dest(row) = -1
        row += 1
      }
      val scattered = batch.columns.map(column => scatter(column, dest, counts)).toVector
      Vector.tabulate(numPartitions) { part =>
        val columns = scattered.map(column => bucketAt(column, part))
        new ColumnBatch(batch.schema, columns, counts(part))
      }

  /**
   * Split one parent bucket into `parts` pieces.
   *
   * The parent exchange already assigned `floorMod(key, parentWidth)`. The piece is
   * `floorMod(floorDiv(key, parentWidth), parts)`, so two keys in that bucket separate when their
   * quotients differ. `floorDiv` rounds toward negative infinity, which keeps a negative key with
   * the same quotient on both sides. A second `floorMod(key, parts)` does not: when `parts`
   * divides `parentWidth`, every key in the bucket has the same residue. Null keys are dropped
   * unless `carryNullKeys` is set. A carried null key goes to partition 0. [[partitionInt32]] is
   * unchanged.
   */
  private[scarlet] def partitionInt32Within(
      batch: ColumnBatch,
      keyOrdinal: Int,
      parentWidth: Int,
      parts: Int,
      carryNullKeys: Boolean = false,
  ): Vector[ColumnBatch] =
    require(parentWidth > 0, s"parentWidth must be positive, got $parentWidth")
    require(parts > 0, s"parts must be positive, got $parts")
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val rows = batch.length
    // One piece already holds every row. Sharing the batch is safe when null keys are kept,
    // and when there is no null key to drop. The quotient is irrelevant at one piece.
    if (parts == 1 && (carryNullKeys || ColumnHash.fullyPresent(keys))) then Vector(batch)
    else
      val dest = new Array[Int](rows)
      val counts = new Array[Int](parts)
      val keyValues = keys.values
      val keyWords = keys.validity.words
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then
          val part = Math.floorMod(Math.floorDiv(keyValues(row), parentWidth), parts)
          dest(row) = part
          counts(part) += 1
        else if (carryNullKeys) then
          dest(row) = nullKeyPartition
          counts(nullKeyPartition) += 1
        else dest(row) = -1
        row += 1
      }
      val scattered = batch.columns.map(column => scatter(column, dest, counts)).toVector
      Vector.tabulate(parts) { part =>
        val columns = scattered.map(column => bucketAt(column, part))
        new ColumnBatch(batch.schema, columns, counts(part))
      }

  /**
   * Partition on an `Int64` key. The bucket is `floorMod(Long.hashCode(key), numPartitions)`,
   * matching `HashPartitioner` for a `Long`. [[partitionInt32]] is unchanged.
   */
  def partitionInt64(
      batch: ColumnBatch,
      keyOrdinal: Int,
      numPartitions: Int,
      carryNullKeys: Boolean = false,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    val keys = ColumnHash.int64At(batch, keyOrdinal)
    val rows = batch.length
    // One partition already holds every row. Sharing the batch is safe when null keys are kept,
    // and when there is no null key to drop.
    if (numPartitions == 1 && (carryNullKeys || ColumnHash.fullyPresent(keys))) then Vector(batch)
    else
      val dest = new Array[Int](rows)
      val counts = new Array[Int](numPartitions)
      val keyValues = keys.values
      val keyWords = keys.validity.words
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then
          val part = Math.floorMod(java.lang.Long.hashCode(keyValues(row)), numPartitions)
          dest(row) = part
          counts(part) += 1
        else if (carryNullKeys) then
          dest(row) = nullKeyPartition
          counts(nullKeyPartition) += 1
        else dest(row) = -1
        row += 1
      }
      val scattered = batch.columns.map(column => scatter(column, dest, counts)).toVector
      Vector.tabulate(numPartitions) { part =>
        val columns = scattered.map(column => bucketAt(column, part))
        new ColumnBatch(batch.schema, columns, counts(part))
      }

  /**
   * Partition every batch, then concatenate bucket `i` across batches. Bucket order follows
   * `batches`.
   */
  def partitionAll(
      batches: Vector[ColumnBatch],
      keyOrdinal: Int,
      numPartitions: Int,
      parallelism: Int,
      carryNullKeys: Boolean = false,
  ): Vector[ColumnBatch] =
    require(batches.nonEmpty, "partitionAll needs at least one batch")
    val scattered = MorselScheduler.map(batches, parallelism) { (_, batch) =>
      partitionInt32(batch, keyOrdinal, numPartitions, carryNullKeys)
    }
    Vector.tabulate(numPartitions) { part =>
      ColumnBatches.concat(scattered.map(buckets => bucketAt(buckets, part)))
    }

  /**
   * [[partitionAll]] for a second split inside a parent bucket. Piece assignment is
   * [[partitionInt32Within]]. A null key is dropped. `parts` buckets are returned, including empty
   * ones, in piece order. [[partitionAll]] is unchanged.
   */
  private[scarlet] def partitionAllWithin(
      batches: Vector[ColumnBatch],
      keyOrdinal: Int,
      parentWidth: Int,
      parts: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(batches.nonEmpty, "partitionAllWithin needs at least one batch")
    val scattered = MorselScheduler.map(batches, parallelism) { (_, batch) =>
      partitionInt32Within(batch, keyOrdinal, parentWidth, parts)
    }
    Vector.tabulate(parts) { part =>
      ColumnBatches.concat(scattered.map(buckets => bucketAt(buckets, part)))
    }

  /**
   * [[partitionAll]] for int pairs addressed by a [[Selected]]. Only `rows(0 until count)` are
   * scattered. `numPartitions == 1` still builds one bucket and does not return the source batch.
   * A null key is dropped. A null value is copied with its validity bit. This is not the dense
   * loop with an identity index.
   */
  def partitionAllSelected(
      selected: Vector[Selected],
      keyOrdinal: Int,
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(selected.nonEmpty, "partitionAllSelected needs at least one batch")
    val scattered = MorselScheduler.map(selected, parallelism) { (_, one) =>
      partitionSelected(one, keyOrdinal, numPartitions)
    }
    Vector.tabulate(numPartitions) { part =>
      ColumnBatches.concat(scattered.map(buckets => bucketAt(buckets, part)))
    }

  /**
   * [[partitionAllUtf8]] for a dictionary string key addressed by a [[Selected]]. Only
   * `rows(0 until count)` are scattered. A null key is dropped. `""` hashes to 0 and is kept. Each
   * bucket shares the source dictionary. `numPartitions == 1` still builds one bucket and does not
   * return the source batch when the selection is not the whole batch. This is not the dense loop
   * with an identity index.
   */
  def partitionAllSelectedUtf8(
      selected: Vector[Selected],
      keyOrdinal: Int,
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(selected.nonEmpty, "partitionAllSelectedUtf8 needs at least one batch")
    val scattered = MorselScheduler.map(selected, parallelism) { (_, one) =>
      partitionSelectedUtf8(one, keyOrdinal, numPartitions)
    }
    Vector.tabulate(numPartitions) { part =>
      ColumnBatches.concat(scattered.map(buckets => bucketAt(buckets, part)))
    }

  /**
   * [[partitionAll]] for an `Int64` key. Bucket assignment is [[partitionInt64]]. `partitionAll`
   * itself still partitions `Int32` keys with `floorMod(key, numPartitions)`.
   */
  def partitionAllInt64(
      batches: Vector[ColumnBatch],
      keyOrdinal: Int,
      numPartitions: Int,
      parallelism: Int,
      carryNullKeys: Boolean = false,
  ): Vector[ColumnBatch] =
    require(batches.nonEmpty, "partitionAllInt64 needs at least one batch")
    val scattered = MorselScheduler.map(batches, parallelism) { (_, batch) =>
      partitionInt64(batch, keyOrdinal, numPartitions, carryNullKeys)
    }
    Vector.tabulate(numPartitions) { part =>
      ColumnBatches.concat(scattered.map(buckets => bucketAt(buckets, part)))
    }

  /**
   * Partition on a dictionary string key. The bucket is
   * `floorMod(StringHash.hash(text), numPartitions)`, matching `HashPartitioner`. The dictionary
   * is hashed once. A clear key bit is dropped unless `carryNullKeys` is set, in which case it
   * goes to partition 0. `""` hashes to 0 and is kept. The default is off.
   */
  def partitionUtf8(
      batch: ColumnBatch,
      keyOrdinal: Int,
      numPartitions: Int,
      carryNullKeys: Boolean = false,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    val keys = utf8At(batch, keyOrdinal)
    val rows = batch.length
    // One partition already holds every row. Sharing the batch is safe when null keys are kept,
    // and when there is no null key to drop.
    if (numPartitions == 1 && (carryNullKeys || ColumnHash.fullyPresent(keys))) then Vector(batch)
    else
      val hashes = StringHash.hashDictionary(keys.dictionary)
      val dest = new Array[Int](rows)
      val counts = new Array[Int](numPartitions)
      val codes = keys.codes
      val keyWords = keys.validity.words
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then
          val part = Math.floorMod(hashes(codes(row)), numPartitions)
          dest(row) = part
          counts(part) += 1
        else if (carryNullKeys) then
          dest(row) = nullKeyPartition
          counts(nullKeyPartition) += 1
        else dest(row) = -1
        row += 1
      }
      val scattered = batch.columns.map(column => scatter(column, dest, counts)).toVector
      Vector.tabulate(numPartitions) { part =>
        val columns = scattered.map(column => bucketAt(column, part))
        new ColumnBatch(batch.schema, columns, counts(part))
      }

  /**
   * [[partitionAll]] for a dictionary string key. Bucket assignment is [[partitionUtf8]].
   * Concatenation remaps unlike dictionaries, so each combined bucket has one dictionary.
   * `carryNullKeys` defaults to false, so a null key is dropped unless the caller opts in.
   */
  def partitionAllUtf8(
      batches: Vector[ColumnBatch],
      keyOrdinal: Int,
      numPartitions: Int,
      parallelism: Int,
      carryNullKeys: Boolean = false,
  ): Vector[ColumnBatch] =
    require(batches.nonEmpty, "partitionAllUtf8 needs at least one batch")
    val scattered = MorselScheduler.map(batches, parallelism) { (_, batch) =>
      partitionUtf8(batch, keyOrdinal, numPartitions, carryNullKeys)
    }
    Vector.tabulate(numPartitions) { part =>
      ColumnBatches.concat(scattered.map(buckets => bucketAt(buckets, part)))
    }

  private def partitionSelected(
      selected: Selected,
      keyOrdinal: Int,
      numPartitions: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    val batch = selected.batch
    val rows = selected.rows
    val live = selected.count
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val dest = new Array[Int](live)
    val counts = new Array[Int](numPartitions)
    val keyValues = keys.values
    val keyWords = keys.validity.words
    var slot = 0
    while (slot < live) {
      val row = rows(slot)
      if (Validity.isSet(keyWords, row)) then
        val part = Math.floorMod(keyValues(row), numPartitions)
        dest(slot) = part
        counts(part) += 1
      else dest(slot) = -1
      slot += 1
    }
    val scattered = batch.columns.map { column =>
      scatterSelected(column, rows, live, dest, counts)
    }.toVector
    Vector.tabulate(numPartitions) { part =>
      val columns = scattered.map(column => bucketAt(column, part))
      new ColumnBatch(batch.schema, columns, counts(part))
    }

  private def partitionSelectedUtf8(
      selected: Selected,
      keyOrdinal: Int,
      numPartitions: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    val batch = selected.batch
    val rows = selected.rows
    val live = selected.count
    val keys = utf8At(batch, keyOrdinal)
    val hashes = StringHash.hashDictionary(keys.dictionary)
    val dest = new Array[Int](live)
    val counts = new Array[Int](numPartitions)
    val codes = keys.codes
    val keyWords = keys.validity.words
    var slot = 0
    while (slot < live) {
      val row = rows(slot)
      if (Validity.isSet(keyWords, row)) then
        val part = Math.floorMod(hashes(codes(row)), numPartitions)
        dest(slot) = part
        counts(part) += 1
      else dest(slot) = -1
      slot += 1
    }
    val scattered = batch.columns.map { column =>
      scatterSelectedUtf8(column, rows, live, dest, counts)
    }.toVector
    Vector.tabulate(numPartitions) { part =>
      val columns = scattered.map(column => bucketAt(column, part))
      new ColumnBatch(batch.schema, columns, counts(part))
    }

  private def utf8At(batch: ColumnBatch, ordinal: Int): DictUtf8Column =
    ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private def bucketAt[A](buckets: Vector[A], part: Int): A =
    buckets.lift(part).getOrElse {
      throw new IllegalArgumentException(s"missing partition $part")
    }

  private def scatter(column: Column, dest: Array[Int], counts: Array[Int]): Vector[Column] =
    column match
      case int32: Int32Column => scatterInt32(int32, dest, counts)
      case int64: Int64Column => scatterInt64(int64, dest, counts)
      case float64: Float64Column => scatterFloat64(float64, dest, counts)
      case bool: BoolColumn => scatterBool(bool, dest, counts)
      case utf8: DictUtf8Column => scatterUtf8(utf8, dest, counts)

  private def scatterInt32(
      column: Int32Column,
      dest: Array[Int],
      counts: Array[Int],
  ): Vector[Column] =
    val outputs = counts.map(count => new Array[Int](count))
    val words = counts.map(count => Validity.allocate(count))
    val cursor = new Array[Int](counts.length)
    val values = column.values
    val validity = column.validity.words
    var row = 0
    while (row < dest.length) {
      val part = dest(row)
      if (part >= 0) then
        val at = cursor(part)
        outputs(part)(at) = values(row)
        if (Validity.isSet(validity, row)) then Validity.setBit(words(part), at)
        cursor(part) = at + 1
      row += 1
    }
    outputs.indices.map { part =>
      Int32Column.of(outputs(part), Validity.fromWords(words(part), counts(part)), counts(part))
    }.toVector

  private def scatterSelected(
      column: Column,
      rows: Array[Int],
      count: Int,
      dest: Array[Int],
      counts: Array[Int],
  ): Vector[Column] =
    column match
      case int32: Int32Column => scatterInt32Selected(int32, rows, count, dest, counts)
      case other =>
        throw new IllegalArgumentException(
          s"selection scatter expected Int32, found ${other.logicalType}",
        )

  /** Same builders as [[scatterInt32]]. Each read is `values(rows(slot))`. */
  private def scatterInt32Selected(
      column: Int32Column,
      rows: Array[Int],
      count: Int,
      dest: Array[Int],
      counts: Array[Int],
  ): Vector[Column] =
    val outputs = counts.map(count => new Array[Int](count))
    val words = counts.map(count => Validity.allocate(count))
    val cursor = new Array[Int](counts.length)
    val values = column.values
    val validity = column.validity.words
    var slot = 0
    while (slot < count) {
      val part = dest(slot)
      if (part >= 0) then
        val at = cursor(part)
        val row = rows(slot)
        outputs(part)(at) = values(row)
        if (Validity.isSet(validity, row)) then Validity.setBit(words(part), at)
        cursor(part) = at + 1
      slot += 1
    }
    outputs.indices.map { part =>
      Int32Column.of(outputs(part), Validity.fromWords(words(part), counts(part)), counts(part))
    }.toVector

  private def scatterInt64(
      column: Int64Column,
      dest: Array[Int],
      counts: Array[Int],
  ): Vector[Column] =
    val outputs = counts.map(count => new Array[Long](count))
    val words = counts.map(count => Validity.allocate(count))
    val cursor = new Array[Int](counts.length)
    val values = column.values
    val validity = column.validity.words
    var row = 0
    while (row < dest.length) {
      val part = dest(row)
      if (part >= 0) then
        val at = cursor(part)
        outputs(part)(at) = values(row)
        if (Validity.isSet(validity, row)) then Validity.setBit(words(part), at)
        cursor(part) = at + 1
      row += 1
    }
    outputs.indices.map { part =>
      Int64Column.of(outputs(part), Validity.fromWords(words(part), counts(part)), counts(part))
    }.toVector

  private def scatterFloat64(
      column: Float64Column,
      dest: Array[Int],
      counts: Array[Int],
  ): Vector[Column] =
    val outputs = counts.map(count => new Array[Double](count))
    val words = counts.map(count => Validity.allocate(count))
    val cursor = new Array[Int](counts.length)
    val values = column.values
    val validity = column.validity.words
    var row = 0
    while (row < dest.length) {
      val part = dest(row)
      if (part >= 0) then
        val at = cursor(part)
        outputs(part)(at) = values(row)
        if (Validity.isSet(validity, row)) then Validity.setBit(words(part), at)
        cursor(part) = at + 1
      row += 1
    }
    outputs.indices.map { part =>
      Float64Column.of(outputs(part), Validity.fromWords(words(part), counts(part)), counts(part))
    }.toVector

  private def scatterBool(
      column: BoolColumn,
      dest: Array[Int],
      counts: Array[Int],
  ): Vector[Column] =
    val outputs = counts.map(count => new Array[Byte](count))
    val words = counts.map(count => Validity.allocate(count))
    val cursor = new Array[Int](counts.length)
    val values = column.values
    val validity = column.validity.words
    var row = 0
    while (row < dest.length) {
      val part = dest(row)
      if (part >= 0) then
        val at = cursor(part)
        outputs(part)(at) = values(row)
        if (Validity.isSet(validity, row)) then Validity.setBit(words(part), at)
        cursor(part) = at + 1
      row += 1
    }
    outputs.indices.map { part =>
      BoolColumn.of(outputs(part), Validity.fromWords(words(part), counts(part)), counts(part))
    }.toVector

  private def scatterUtf8(
      column: DictUtf8Column,
      dest: Array[Int],
      counts: Array[Int],
  ): Vector[Column] =
    val outputs = counts.map(count => new Array[Int](count))
    val words = counts.map(count => Validity.allocate(count))
    val cursor = new Array[Int](counts.length)
    val values = column.codes
    val validity = column.validity.words
    var row = 0
    while (row < dest.length) {
      val part = dest(row)
      if (part >= 0) then
        val at = cursor(part)
        outputs(part)(at) = values(row)
        if (Validity.isSet(validity, row)) then Validity.setBit(words(part), at)
        cursor(part) = at + 1
      row += 1
    }
    outputs.indices.map { part =>
      DictUtf8Column.of(
        column.dictionary,
        outputs(part),
        Validity.fromWords(words(part), counts(part)),
        counts(part),
      )
    }.toVector

  private def scatterSelectedUtf8(
      column: Column,
      rows: Array[Int],
      count: Int,
      dest: Array[Int],
      counts: Array[Int],
  ): Vector[Column] =
    column match
      case utf8: DictUtf8Column => scatterUtf8Rows(utf8, rows, count, dest, counts)
      case int32: Int32Column => scatterInt32Selected(int32, rows, count, dest, counts)
      case other =>
        throw new IllegalArgumentException(
          s"utf8 selection scatter expected Utf8Dict or Int32, found ${other.logicalType}",
        )

  /**
   * Same builders as [[scatterUtf8]]. Each read is `codes(rows(slot))`. The dictionary is shared.
   */
  private def scatterUtf8Rows(
      column: DictUtf8Column,
      rows: Array[Int],
      count: Int,
      dest: Array[Int],
      counts: Array[Int],
  ): Vector[Column] =
    val outputs = counts.map(count => new Array[Int](count))
    val words = counts.map(count => Validity.allocate(count))
    val cursor = new Array[Int](counts.length)
    val values = column.codes
    val validity = column.validity.words
    var slot = 0
    while (slot < count) {
      val part = dest(slot)
      if (part >= 0) then
        val at = cursor(part)
        val row = rows(slot)
        outputs(part)(at) = values(row)
        if (Validity.isSet(validity, row)) then Validity.setBit(words(part), at)
        cursor(part) = at + 1
      slot += 1
    }
    outputs.indices.map { part =>
      DictUtf8Column.of(
        column.dictionary,
        outputs(part),
        Validity.fromWords(words(part), counts(part)),
        counts(part),
      )
    }.toVector
