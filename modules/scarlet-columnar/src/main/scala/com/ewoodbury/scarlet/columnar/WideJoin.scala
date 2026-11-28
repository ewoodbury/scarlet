package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.{ColumnField, ColumnKind, ScarletConf}

/**
 * Inner join of wide batches on one int or long key.
 *
 * The match is the existing int or long hash join. Each side's value column is the row index, so
 * the hash table stays the one those kernels already build. Payload columns are copied by that
 * index. A null key is not inserted and is not probed. A null payload is copied.
 *
 * Hash mode joins each shuffle bucket with itself. When spill is on, a hash bucket whose int or
 * long build estimate is over the spill budget is written with the existing spill store and read
 * back. Float, bool, and float32 columns stay in memory. Spill off writes nothing. Broadcast mode
 * hashes the right side once and probes every left batch. Output is every left column, then every
 * right column except the key. A left outer join keeps each present left key. A missing partner is
 * invalid. A stored zero stays present.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object WideJoin:

  def hash(
      left: Seq[ColumnBatch],
      right: Seq[ColumnBatch],
      leftFields: Vector[ColumnField],
      rightFields: Vector[ColumnField],
      leftKey: Int,
      rightKey: Int,
  ): (Vector[ColumnField], ColumnBatch) =
    val kind = keyKind(leftFields, rightFields, leftKey, rightKey)
    val longKey = kind == ColumnKind.Int64
    val width = math.max(ScarletConf.get.defaultShufflePartitions, 1)
    val leftParts = WideExchange.hold(split(left, leftKey, kind, width), longKey)
    val rightParts = WideExchange.hold(split(right, rightKey, kind, width), longKey)
    val matched = (0 until width).iterator.flatMap { part =>
      local(leftParts(part), rightParts(part), leftKey, rightKey, kind)
    }.toSeq
    (
      outputFields(leftFields, rightFields, rightKey),
      concat(matched, leftFields, rightFields, rightKey),
    )

  def broadcast(
      left: Seq[ColumnBatch],
      right: Seq[ColumnBatch],
      leftFields: Vector[ColumnField],
      rightFields: Vector[ColumnField],
      leftKey: Int,
      rightKey: Int,
  ): (Vector[ColumnField], ColumnBatch) =
    val kind = keyKind(leftFields, rightFields, leftKey, rightKey)
    val build = concat(right, rightFields, Vector.empty, -1)
    val matched = left.iterator
      .filter(_.length > 0)
      .flatMap { probe =>
        local(Seq(probe), Seq(build), leftKey, rightKey, kind)
      }
      .toSeq
    (
      outputFields(leftFields, rightFields, rightKey),
      concat(matched, leftFields, rightFields, rightKey),
    )

  /**
   * Left outer hash join. Hash and broadcast name the same multiset. A null key is dropped. A
   * missing partner is invalid, and a stored zero stays present.
   */
  def hashLeft(
      left: Seq[ColumnBatch],
      right: Seq[ColumnBatch],
      leftFields: Vector[ColumnField],
      rightFields: Vector[ColumnField],
      leftKey: Int,
      rightKey: Int,
  ): (Vector[ColumnField], ColumnBatch) =
    val kind = keyKind(leftFields, rightFields, leftKey, rightKey)
    val longKey = kind == ColumnKind.Int64
    val width = math.max(ScarletConf.get.defaultShufflePartitions, 1)
    val prototype = concat(right, rightFields, Vector.empty, -1)
    val leftParts = WideExchange.hold(split(left, leftKey, kind, width), longKey)
    val rightParts = WideExchange.hold(split(right, rightKey, kind, width), longKey)
    val matched = (0 until width).iterator.flatMap { part =>
      localLeft(leftParts(part), rightParts(part), prototype, leftKey, rightKey, kind)
    }.toSeq
    (
      outputFields(leftFields, rightFields, rightKey),
      concat(matched, leftFields, rightFields, rightKey),
    )

  /** Left outer broadcast join. The build is hashed once and probed from every left batch. */
  def broadcastLeft(
      left: Seq[ColumnBatch],
      right: Seq[ColumnBatch],
      leftFields: Vector[ColumnField],
      rightFields: Vector[ColumnField],
      leftKey: Int,
      rightKey: Int,
  ): (Vector[ColumnField], ColumnBatch) =
    val kind = keyKind(leftFields, rightFields, leftKey, rightKey)
    val prototype = concat(right, rightFields, Vector.empty, -1)
    val matched = left.iterator
      .filter(_.length > 0)
      .flatMap { probe =>
        localLeft(Seq(probe), Seq(prototype), prototype, leftKey, rightKey, kind)
      }
      .toSeq
    (
      outputFields(leftFields, rightFields, rightKey),
      concat(matched, leftFields, rightFields, rightKey),
    )

  /** Left columns, then right columns except the key. */
  def outputFields(
      left: Vector[ColumnField],
      right: Vector[ColumnField],
      rightKey: Int,
  ): Vector[ColumnField] =
    left ++ right.zipWithIndex.collect { case (field, ordinal) if ordinal != rightKey => field }

  private def local(
      left: Seq[ColumnBatch],
      right: Seq[ColumnBatch],
      leftKey: Int,
      rightKey: Int,
      kind: ColumnKind,
  ): Seq[ColumnBatch] =
    val probe = concat(left, Vector.empty, Vector.empty, -1)
    val build = concat(right, Vector.empty, Vector.empty, -1)
    if (probe.length == 0 || build.length == 0) then Seq.empty
    else Seq(matchRows(probe, build, leftKey, rightKey, kind))

  private def localLeft(
      left: Seq[ColumnBatch],
      right: Seq[ColumnBatch],
      prototype: ColumnBatch,
      probeKey: Int,
      buildKey: Int,
      kind: ColumnKind,
  ): Seq[ColumnBatch] =
    val probe = concat(left, Vector.empty, Vector.empty, -1)
    if (probe.length == 0) then Seq.empty
    else
      val build = concat(right, Vector.empty, Vector.empty, -1)
      val hits = new Array[Boolean](probe.length)
      val matched =
        if (build.length == 0) then None
        else Some(matchRows(probe, build, probeKey, buildKey, kind, Some(hits)))
      val extra = preserve(probe, prototype, probeKey, buildKey, hits)
      val pieces = Vector.newBuilder[ColumnBatch]
      matched.foreach { batch =>
        if (batch.length > 0) then pieces += batch
      }
      if (extra.length > 0) then pieces += extra
      pieces.result()

  private def matchRows(
      probe: ColumnBatch,
      build: ColumnBatch,
      probeKey: Int,
      buildKey: Int,
      kind: ColumnKind,
      hit: Option[Array[Boolean]] = None,
  ): ColumnBatch =
    val joined = kind match
      case ColumnKind.Int32 =>
        HashJoin.innerInt32(
          HashJoin.JoinSide(indexed(build, buildKey, longKey = false), 0, 1),
          HashJoin.JoinSide(indexed(probe, probeKey, longKey = false), 0, 1),
        )
      case ColumnKind.Int64 =>
        LongHashJoin.innerInt64(
          HashJoin.JoinSide(indexed(build, buildKey, longKey = true), 0, 1),
          HashJoin.JoinSide(indexed(probe, probeKey, longKey = true), 0, 1),
        )
      case other =>
        throw new IllegalArgumentException(s"wide join key must be int or long, found $other")
    val rows = joined.length
    val buildAt = new Array[Int](rows)
    val probeAt = new Array[Int](rows)
    if (kind == ColumnKind.Int64) then
      val buildRows = ColumnHash.int64At(joined, 1).values
      val probeRows = ColumnHash.int64At(joined, 2).values
      var row = 0
      while (row < rows) {
        buildAt(row) = buildRows(row).toInt
        probeAt(row) = probeRows(row).toInt
        row += 1
      }
    else
      val buildRows = ColumnHash.int32At(joined, 1).values
      val probeRows = ColumnHash.int32At(joined, 2).values
      var row = 0
      while (row < rows) {
        buildAt(row) = buildRows(row)
        probeAt(row) = probeRows(row)
        row += 1
      }
    hit.foreach { flags =>
      var mark = 0
      while (mark < rows) {
        flags(probeAt(mark)) = true
        mark += 1
      }
    }
    val columns = Vector.newBuilder[Column]
    var ordinal = 0
    while (ordinal < probe.columns.size) {
      columns += gather(probe.columns(ordinal), probeAt, rows)
      ordinal += 1
    }
    ordinal = 0
    while (ordinal < build.columns.size) {
      if (ordinal != buildKey) then columns += gather(build.columns(ordinal), buildAt, rows)
      ordinal += 1
    }
    val out = columns.result()
    new ColumnBatch(out.map(_.logicalType), out, rows)

  private def indexed(batch: ColumnBatch, keyOrdinal: Int, longKey: Boolean): ColumnBatch =
    val rows = batch.length
    if (longKey) then
      val ids = new Array[Long](rows)
      var row = 0
      while (row < rows) {
        ids(row) = row.toLong
        row += 1
      }
      val key = ColumnHash.int64At(batch, keyOrdinal)
      val index = Int64Column.of(ids, Validity.allValid(rows), rows)
      new ColumnBatch(Vector(LogicalType.Int64, LogicalType.Int64), Vector(key, index), rows)
    else
      val ids = new Array[Int](rows)
      var row = 0
      while (row < rows) {
        ids(row) = row
        row += 1
      }
      val key = ColumnHash.int32At(batch, keyOrdinal)
      val index = Int32Column.of(ids, Validity.allValid(rows), rows)
      new ColumnBatch(Vector(LogicalType.Int32, LogicalType.Int32), Vector(key, index), rows)

  private def split(
      batches: Seq[ColumnBatch],
      keyOrdinal: Int,
      kind: ColumnKind,
      width: Int,
  ): Array[Seq[ColumnBatch]] =
    val parts = Array.fill(width)(Seq.empty[ColumnBatch])
    batches.foreach { batch =>
      if (batch.length > 0) then
        val owned = Array.fill(width)(new scala.collection.mutable.ArrayBuffer[Int])
        var row = 0
        while (row < batch.length) {
          if (present(batch, keyOrdinal, row)) then
            val part = bucket(batch, keyOrdinal, kind, row, width)
            owned(part) += row
          row += 1
        }
        var part = 0
        while (part < width) {
          if (owned(part).nonEmpty) then
            parts(part) = parts(part) :+ gatherRows(batch, owned(part).toArray)
          part += 1
        }
    }
    parts

  private def bucket(
      batch: ColumnBatch,
      keyOrdinal: Int,
      kind: ColumnKind,
      row: Int,
      width: Int,
  ): Int =
    val hash = kind match
      case ColumnKind.Int64 =>
        java.lang.Long.hashCode(ColumnHash.int64At(batch, keyOrdinal).values(row))
      case _ =>
        ColumnHash.int32At(batch, keyOrdinal).values(row)
    Math.floorMod(hash, width)

  private def present(batch: ColumnBatch, ordinal: Int, row: Int): Boolean =
    batch.columns(ordinal).isValid(row)

  /** Present probe keys with no build hit. Right non-key columns are invalid. */
  private def preserve(
      probe: ColumnBatch,
      prototype: ColumnBatch,
      probeKey: Int,
      buildKey: Int,
      hit: Array[Boolean],
  ): ColumnBatch =
    val keep = new scala.collection.mutable.ArrayBuffer[Int]
    var row = 0
    while (row < probe.length) {
      if (!hit(row) && present(probe, probeKey, row)) then keep += row
      row += 1
    }
    val chosen = keep.toArray
    val length = chosen.length
    if (length == 0) then new ColumnBatch(Vector.empty, Vector.empty, 0)
    else
      val columns = Vector.newBuilder[Column]
      var ordinal = 0
      while (ordinal < probe.columns.size) {
        columns += gather(probe.columns(ordinal), chosen, length)
        ordinal += 1
      }
      ordinal = 0
      while (ordinal < prototype.columns.size) {
        if (ordinal != buildKey) then columns += absent(prototype.columns(ordinal), length)
        ordinal += 1
      }
      val out = columns.result()
      new ColumnBatch(out.map(_.logicalType), out, length)

  private def absent(column: Column, length: Int): Column =
    val missing = Validity.pack(length, _ => false)
    column match
      case _: Int32Column =>
        Int32Column.of(new Array[Int](length), missing, length)
      case _: Int64Column =>
        Int64Column.of(new Array[Long](length), missing, length)
      case _: Float64Column =>
        Float64Column.of(new Array[Double](length), missing, length)
      case _: Float32Column =>
        Float32Column.of(new Array[Float](length), missing, length)
      case _: BoolColumn =>
        BoolColumn.of(new Array[Byte](length), missing, length)
      case source: DictUtf8Column =>
        DictUtf8Column.of(source.dictionary, new Array[Int](length), missing, length)
      case source: DecimalColumn =>
        DecimalColumn.of(source.precision, source.scale, new Array[Long](length), missing, length)
      case other =>
        throw new IllegalArgumentException(s"${other.logicalType} is not supported here")

  private def gatherRows(batch: ColumnBatch, rows: Array[Int]): ColumnBatch =
    val columns = batch.columns.map(column => gather(column, rows, rows.length))
    new ColumnBatch(columns.map(_.logicalType), columns, rows.length)

  private def gather(column: Column, rows: Array[Int], length: Int): Column =
    val present = new Array[Boolean](length)
    column match
      case source: Int32Column =>
        val values = new Array[Int](length)
        copyInts(source.values, source, rows, values, present, length)
        Int32Column.of(values, Validity.pack(length, row => present(row)), length)
      case source: Int64Column =>
        val values = new Array[Long](length)
        var row = 0
        while (row < length) {
          val at = rows(row)
          if (source.isValid(at)) then
            values(row) = source.values(at)
            present(row) = true
          row += 1
        }
        Int64Column.of(values, Validity.pack(length, row => present(row)), length)
      case source: Float64Column =>
        val values = new Array[Double](length)
        var row = 0
        while (row < length) {
          val at = rows(row)
          if (source.isValid(at)) then
            values(row) = source.values(at)
            present(row) = true
          row += 1
        }
        Float64Column.of(values, Validity.pack(length, row => present(row)), length)
      case source: Float32Column =>
        val values = new Array[Float](length)
        var row = 0
        while (row < length) {
          val at = rows(row)
          if (source.isValid(at)) then
            values(row) = source.values(at)
            present(row) = true
          row += 1
        }
        Float32Column.of(values, Validity.pack(length, row => present(row)), length)
      case source: BoolColumn =>
        val values = new Array[Byte](length)
        var row = 0
        while (row < length) {
          val at = rows(row)
          if (source.isValid(at)) then
            values(row) = source.values(at)
            present(row) = true
          row += 1
        }
        BoolColumn.of(values, Validity.pack(length, row => present(row)), length)
      case source: DictUtf8Column =>
        val codes = new Array[Int](length)
        var row = 0
        while (row < length) {
          val at = rows(row)
          if (source.isValid(at)) then
            codes(row) = source.codes(at)
            present(row) = true
          row += 1
        }
        DictUtf8Column.of(
          source.dictionary,
          codes,
          Validity.pack(length, row => present(row)),
          length,
        )
      case source: DecimalColumn =>
        DecimalColumn.gather(source, rows, length, length)

  private def copyInts(
      source: Array[Int],
      column: Int32Column,
      rows: Array[Int],
      dest: Array[Int],
      present: Array[Boolean],
      length: Int,
  ): Unit =
    var row = 0
    while (row < length) {
      val at = rows(row)
      if (column.isValid(at)) then
        dest(row) = source(at)
        present(row) = true
      row += 1
    }

  private def concat(
      batches: Seq[ColumnBatch],
      leftFields: Vector[ColumnField],
      rightFields: Vector[ColumnField],
      rightKey: Int,
  ): ColumnBatch =
    val live = batches.filter(_.length > 0)
    if (live.isEmpty) then empty(leftFields, rightFields, rightKey)
    else if (live.sizeIs == 1) then live.head
    else
      val total = live.map(_.length.toLong).sum
      require(total <= Int.MaxValue.toLong, s"wide join has $total rows")
      val rows = total.toInt
      val width = live.head.columns.size
      val columns = (0 until width).map { ordinal =>
        val pieces = live.map(_.columns(ordinal)).toVector
        append(pieces, rows)
      }.toVector
      new ColumnBatch(columns.map(_.logicalType), columns, rows)

  private def empty(
      leftFields: Vector[ColumnField],
      rightFields: Vector[ColumnField],
      rightKey: Int,
  ): ColumnBatch =
    val fields = outputFields(leftFields, rightFields, rightKey)
    val columns = fields.map(field => emptyColumn(field.kind))
    new ColumnBatch(columns.map(_.logicalType), columns, 0)

  private def emptyColumn(kind: ColumnKind): Column = kind match
    case ColumnKind.Int32 => Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0)
    case ColumnKind.Int64 => Int64Column.of(Array.emptyLongArray, Validity.allValid(0), 0)
    case ColumnKind.Float64 => Float64Column.of(Array.emptyDoubleArray, Validity.allValid(0), 0)
    case ColumnKind.Float32 => Float32Column.of(Array.emptyFloatArray, Validity.allValid(0), 0)
    case ColumnKind.Bool => BoolColumn.of(Array.emptyByteArray, Validity.allValid(0), 0)
    case ColumnKind.Utf8 =>
      DictUtf8Column.of(Array.empty[String], Array.emptyIntArray, Validity.allValid(0), 0)
    case ColumnKind.Decimal(precision, scale) =>
      DecimalColumn.of(precision, scale, Array.emptyLongArray, Validity.allValid(0), 0)

  private def append(pieces: Vector[Column], rows: Int): Column =
    pieces.head match
      case _: Int32Column =>
        val values = new Array[Int](rows)
        val present = new Array[Boolean](rows)
        fill(pieces, rows, present) { (column, from, at) =>
          column match
            case source: Int32Column => values(from) = source.values(at)
            case other => mismatch(other)
        }
        Int32Column.of(values, Validity.pack(rows, row => present(row)), rows)
      case _: Int64Column =>
        val values = new Array[Long](rows)
        val present = new Array[Boolean](rows)
        fill(pieces, rows, present) { (column, from, at) =>
          column match
            case source: Int64Column => values(from) = source.values(at)
            case other => mismatch(other)
        }
        Int64Column.of(values, Validity.pack(rows, row => present(row)), rows)
      case _: Float64Column =>
        val values = new Array[Double](rows)
        val present = new Array[Boolean](rows)
        fill(pieces, rows, present) { (column, from, at) =>
          column match
            case source: Float64Column => values(from) = source.values(at)
            case other => mismatch(other)
        }
        Float64Column.of(values, Validity.pack(rows, row => present(row)), rows)
      case _: Float32Column =>
        val values = new Array[Float](rows)
        val present = new Array[Boolean](rows)
        fill(pieces, rows, present) { (column, from, at) =>
          column match
            case source: Float32Column => values(from) = source.values(at)
            case other => mismatch(other)
        }
        Float32Column.of(values, Validity.pack(rows, row => present(row)), rows)
      case _: BoolColumn =>
        val values = new Array[Byte](rows)
        val present = new Array[Boolean](rows)
        fill(pieces, rows, present) { (column, from, at) =>
          column match
            case source: BoolColumn => values(from) = source.values(at)
            case other => mismatch(other)
        }
        BoolColumn.of(values, Validity.pack(rows, row => present(row)), rows)
      case first: DecimalColumn =>
        pieces.foreach {
          case source: DecimalColumn =>
            require(
              source.precision == first.precision && source.scale == first.scale,
              "wide join cannot concatenate decimals with different precision or scale",
            )
          case other =>
            mismatch(other)
        }
        val values = new Array[Long](rows)
        val present = new Array[Boolean](rows)
        fill(pieces, rows, present) { (column, from, at) =>
          column match
            case source: DecimalColumn => values(from) = source.values(at)
            case other => mismatch(other)
        }
        DecimalColumn.of(
          first.precision,
          first.scale,
          values,
          Validity.pack(rows, row => present(row)),
          rows,
        )
      case first: DictUtf8Column =>
        pieces.foreach {
          case source: DictUtf8Column =>
            require(
              sameDictionary(source.dictionary, first.dictionary),
              "wide join cannot concatenate string columns with different dictionaries",
            )
          case other =>
            mismatch(other)
        }
        val codes = new Array[Int](rows)
        val present = new Array[Boolean](rows)
        fill(pieces, rows, present) { (column, from, at) =>
          column match
            case source: DictUtf8Column => codes(from) = source.codes(at)
            case other => mismatch(other)
        }
        DictUtf8Column.of(first.dictionary, codes, Validity.pack(rows, row => present(row)), rows)
      case other =>
        throw new IllegalArgumentException(s"${other.logicalType} is not supported here")

  private def mismatch(column: Column): Nothing =
    throw new IllegalArgumentException(s"wide join column changed type to ${column.logicalType}")

  private def sameDictionary(left: Array[String], right: Array[String]): Boolean =
    if (left.length != right.length) then false
    else
      var index = 0
      var same = true
      while (index < left.length && same) {
        if (!left(index).equals(right(index))) then same = false
        index += 1
      }
      same

  private def fill(
      pieces: Vector[Column],
      rows: Int,
      present: Array[Boolean],
  )(copy: (Column, Int, Int) => Unit): Unit =
    var from = 0
    pieces.foreach { column =>
      var row = 0
      while (row < column.length) {
        if (column.isValid(row)) then
          copy(column, from, row)
          present(from) = true
        from += 1
        row += 1
      }
    }
    require(from == rows, s"concatenated $from rows, expected $rows")

  private def keyKind(
      leftFields: Vector[ColumnField],
      rightFields: Vector[ColumnField],
      leftKey: Int,
      rightKey: Int,
  ): ColumnKind =
    require(leftKey >= 0 && leftKey < leftFields.size, s"left key $leftKey is outside the row")
    require(
      rightKey >= 0 && rightKey < rightFields.size,
      s"right key $rightKey is outside the row",
    )
    val kind = leftFields(leftKey).kind
    require(
      kind == rightFields(rightKey).kind,
      s"join keys are $kind and ${rightFields(rightKey).kind}",
    )
    require(
      kind == ColumnKind.Int32 || kind == ColumnKind.Int64,
      s"wide join key must be int or long, found $kind",
    )
    kind
