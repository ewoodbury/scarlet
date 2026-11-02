package com.ewoodbury.scarlet.columnar

/**
 * One pass over a stack of filters on a multi-column batch.
 *
 * [[com.ewoodbury.scarlet.execution.FilterChain]] uses this for pair and triple batches. A
 * single-column batch stays on chained kernels. The survivor index is four bytes per row, which
 * costs more than the intermediate column a chained kernel writes for the next filter to read. A
 * boolean column is one bit per row.
 *
 * The survivor index is an `Array[Int]` of absolute row ids in the source batch, plus a count. The
 * array is allocated once for the morsel invocation and reused for every step. It is not a field:
 * morsels run in parallel, and each invocation owns its scratch. The first predicate seeds the
 * index. Each later predicate reads those ids and writes the rows it keeps back into the same
 * array, so the count shrinks. A later predicate is a per-row call on the survivors. It is not a
 * word-level AND.
 *
 * A primitive, boolean, or pair slot skips a null and does not call the predicate. A present zero
 * is still a candidate. A key or value slot looks at that ordinal only, so a null on the other
 * ordinal can survive. A pair row needs both ordinals. A triple needs all three present and names
 * the absolute row when one is null, before any predicate call. A general string predicate is
 * called on every candidate, including an empty cell. An empty cell that passes stays absent in
 * the output.
 *
 * Compact gathers every column by absolute row and copies the source validity bit. A clear bit
 * stores zero and leaves the output bit clear. [[Validity.prefixValid]] is not used: a survivor
 * may be absent, and a dense present prefix is what the gather writes when every survivor is
 * present. The output arrays are sized to the morsel. The batch length is the survivor count.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object Selection:

  enum Step:
    case Int32(column: Int, predicate: ColumnKernel.Int32Predicate)
    case Int64(column: Int, predicate: ColumnKernel.Int64Predicate)
    case Float64(column: Int, predicate: ColumnKernel.Float64Predicate)
    case Bool(column: Int, predicate: ColumnKernel.BoolPredicate)
    case Utf8(column: Int, predicate: ColumnKernel.Utf8Predicate)
    case IntPair(predicate: PairKernel.IntPairPredicate)
    case IntTriple(column: Int, predicate: ColumnKernel.Int32Predicate)

  /**
   * Applies `steps` to `[start, start + length)`. `steps` is read in order and is not copied. An
   * empty range publishes an empty batch and does not call a predicate.
   */
  def apply(batch: ColumnBatch, start: Int, length: Int, steps: Seq[Step]): ColumnBatch =
    // The range message names the window. `require` is the check for an empty stack.
    Ranges.check(start, length, batch.length)
    require(steps.nonEmpty, "a filter stack needs a predicate")
    val index = new Array[Int](length)
    val iterator = steps.iterator
    var count = seed(batch, start, length, iterator.next(), index)
    while (iterator.hasNext) {
      count = restrict(batch, iterator.next(), index, count)
    }
    publish(batch, index, count, length)

  private def seed(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      step: Step,
      index: Array[Int],
  ): Int = step match
    case Step.Int32(ordinal, predicate) =>
      seedInt32(batch, ordinal, start, length, predicate, index)
    case Step.Int64(ordinal, predicate) =>
      seedInt64(batch, ordinal, start, length, predicate, index)
    case Step.Float64(ordinal, predicate) =>
      seedFloat64(batch, ordinal, start, length, predicate, index)
    case Step.Bool(ordinal, predicate) =>
      seedBool(batch, ordinal, start, length, predicate, index)
    case Step.Utf8(ordinal, predicate) =>
      seedUtf8(batch, ordinal, start, length, predicate, index)
    case Step.IntPair(predicate) =>
      seedPair(batch, start, length, predicate, index)
    case Step.IntTriple(ordinal, predicate) =>
      seedTriple(batch, ordinal, start, length, predicate, index)

  private def restrict(batch: ColumnBatch, step: Step, index: Array[Int], count: Int): Int =
    step match
      case Step.Int32(ordinal, predicate) =>
        restrictInt32(batch, ordinal, predicate, index, count)
      case Step.Int64(ordinal, predicate) =>
        restrictInt64(batch, ordinal, predicate, index, count)
      case Step.Float64(ordinal, predicate) =>
        restrictFloat64(batch, ordinal, predicate, index, count)
      case Step.Bool(ordinal, predicate) =>
        restrictBool(batch, ordinal, predicate, index, count)
      case Step.Utf8(ordinal, predicate) =>
        restrictUtf8(batch, ordinal, predicate, index, count)
      case Step.IntPair(predicate) =>
        restrictPair(batch, predicate, index, count)
      case Step.IntTriple(ordinal, predicate) =>
        restrictTriple(batch, ordinal, predicate, index, count)

  private def seedInt32(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: ColumnKernel.Int32Predicate,
      index: Array[Int],
  ): Int =
    val column = int32At(batch, ordinal)
    Ranges.check(start, length, column.length)
    val values = column.values
    val words = column.validity.words
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      if (Validity.isSet(words, row) && predicate(values(row))) then
        index(written) = row
        written += 1
      row += 1
    }
    written

  private def restrictInt32(
      batch: ColumnBatch,
      ordinal: Int,
      predicate: ColumnKernel.Int32Predicate,
      index: Array[Int],
      count: Int,
  ): Int =
    val column = int32At(batch, ordinal)
    val values = column.values
    val words = column.validity.words
    var read = 0
    var written = 0
    while (read < count) {
      val row = index(read)
      if (Validity.isSet(words, row) && predicate(values(row))) then
        index(written) = row
        written += 1
      read += 1
    }
    written

  private def seedInt64(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: ColumnKernel.Int64Predicate,
      index: Array[Int],
  ): Int =
    val column = ColumnKernel.asInt64(ColumnKernel.columnAt(batch, ordinal), ordinal)
    Ranges.check(start, length, column.length)
    val values = column.values
    val words = column.validity.words
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      if (Validity.isSet(words, row) && predicate(values(row))) then
        index(written) = row
        written += 1
      row += 1
    }
    written

  private def restrictInt64(
      batch: ColumnBatch,
      ordinal: Int,
      predicate: ColumnKernel.Int64Predicate,
      index: Array[Int],
      count: Int,
  ): Int =
    val column = ColumnKernel.asInt64(ColumnKernel.columnAt(batch, ordinal), ordinal)
    val values = column.values
    val words = column.validity.words
    var read = 0
    var written = 0
    while (read < count) {
      val row = index(read)
      if (Validity.isSet(words, row) && predicate(values(row))) then
        index(written) = row
        written += 1
      read += 1
    }
    written

  private def seedFloat64(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: ColumnKernel.Float64Predicate,
      index: Array[Int],
  ): Int =
    val column = ColumnKernel.asFloat64(ColumnKernel.columnAt(batch, ordinal), ordinal)
    Ranges.check(start, length, column.length)
    val values = column.values
    val words = column.validity.words
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      if (Validity.isSet(words, row) && predicate(values(row))) then
        index(written) = row
        written += 1
      row += 1
    }
    written

  private def restrictFloat64(
      batch: ColumnBatch,
      ordinal: Int,
      predicate: ColumnKernel.Float64Predicate,
      index: Array[Int],
      count: Int,
  ): Int =
    val column = ColumnKernel.asFloat64(ColumnKernel.columnAt(batch, ordinal), ordinal)
    val values = column.values
    val words = column.validity.words
    var read = 0
    var written = 0
    while (read < count) {
      val row = index(read)
      if (Validity.isSet(words, row) && predicate(values(row))) then
        index(written) = row
        written += 1
      read += 1
    }
    written

  private def seedBool(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: ColumnKernel.BoolPredicate,
      index: Array[Int],
  ): Int =
    val column = boolAt(batch, ordinal)
    Ranges.check(start, length, column.length)
    val values = column.values
    val words = column.validity.words
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      if (Validity.isSet(words, row) && predicate(values(row) != 0)) then
        index(written) = row
        written += 1
      row += 1
    }
    written

  private def restrictBool(
      batch: ColumnBatch,
      ordinal: Int,
      predicate: ColumnKernel.BoolPredicate,
      index: Array[Int],
      count: Int,
  ): Int =
    val column = boolAt(batch, ordinal)
    val values = column.values
    val words = column.validity.words
    var read = 0
    var written = 0
    while (read < count) {
      val row = index(read)
      if (Validity.isSet(words, row) && predicate(values(row) != 0)) then
        index(written) = row
        written += 1
      read += 1
    }
    written

  private def seedUtf8(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: ColumnKernel.Utf8Predicate,
      index: Array[Int],
  ): Int =
    val column = ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, ordinal), ordinal)
    Ranges.check(start, length, column.length)
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      if (predicate(ColumnKernel.utf8String(column, row))) then
        index(written) = row
        written += 1
      row += 1
    }
    written

  private def restrictUtf8(
      batch: ColumnBatch,
      ordinal: Int,
      predicate: ColumnKernel.Utf8Predicate,
      index: Array[Int],
      count: Int,
  ): Int =
    val column = ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, ordinal), ordinal)
    var read = 0
    var written = 0
    while (read < count) {
      val row = index(read)
      if (predicate(ColumnKernel.utf8String(column, row))) then
        index(written) = row
        written += 1
      read += 1
    }
    written

  private def seedPair(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      predicate: PairKernel.IntPairPredicate,
      index: Array[Int],
  ): Int =
    val (keys, values) = pairColumns(batch)
    Ranges.check(start, length, keys.length)
    val keyValues = keys.values
    val valueValues = values.values
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      if (bothPresent(keyWords, valueWords, row) && predicate(keyValues(row), valueValues(row)))
      then
        index(written) = row
        written += 1
      row += 1
    }
    written

  private def restrictPair(
      batch: ColumnBatch,
      predicate: PairKernel.IntPairPredicate,
      index: Array[Int],
      count: Int,
  ): Int =
    val (keys, values) = pairColumns(batch)
    val keyValues = keys.values
    val valueValues = values.values
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    var read = 0
    var written = 0
    while (read < count) {
      val row = index(read)
      if (bothPresent(keyWords, valueWords, row) && predicate(keyValues(row), valueValues(row)))
      then
        index(written) = row
        written += 1
      read += 1
    }
    written

  private def seedTriple(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: ColumnKernel.Int32Predicate,
      index: Array[Int],
  ): Int =
    val (keys, left, right) = tripleColumns(batch)
    Ranges.check(start, length, keys.length)
    requireTripleRange(keys, left, right, start, length)
    seedInt32(batch, ordinal, start, length, predicate, index)

  private def restrictTriple(
      batch: ColumnBatch,
      ordinal: Int,
      predicate: ColumnKernel.Int32Predicate,
      index: Array[Int],
      count: Int,
  ): Int =
    val (keys, left, right) = tripleColumns(batch)
    requireTripleIds(keys, left, right, index, count)
    restrictInt32(batch, ordinal, predicate, index, count)

  private def requireTripleRange(
      keys: Int32Column,
      left: Int32Column,
      right: Int32Column,
      start: Int,
      length: Int,
  ): Unit =
    val end = start + length
    var row = start
    while (row < end) {
      require(triplePresent(keys, left, right, row), s"null Int32 join value at row $row")
      row += 1
    }

  private def requireTripleIds(
      keys: Int32Column,
      left: Int32Column,
      right: Int32Column,
      index: Array[Int],
      count: Int,
  ): Unit =
    var read = 0
    while (read < count) {
      val row = index(read)
      require(triplePresent(keys, left, right, row), s"null Int32 join value at row $row")
      read += 1
    }

  private def triplePresent(
      keys: Int32Column,
      left: Int32Column,
      right: Int32Column,
      row: Int,
  ): Boolean =
    Validity.isSet(keys.validity.words, row) &&
      Validity.isSet(left.validity.words, row) &&
      Validity.isSet(right.validity.words, row)

  private inline def bothPresent(
      keyWords: Array[Long],
      valueWords: Array[Long],
      row: Int,
  ): Boolean =
    Validity.isSet(keyWords, row) && Validity.isSet(valueWords, row)

  private def pairColumns(batch: ColumnBatch): (Int32Column, Int32Column) =
    require(
      batch.columns.length == 2,
      s"pair filter expected 2 columns, got ${batch.columns.length}",
    )
    val keys = int32At(batch, 0)
    val values = int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    (keys, values)

  private def tripleColumns(batch: ColumnBatch): (Int32Column, Int32Column, Int32Column) =
    require(
      batch.columns.length == 3,
      s"triple filter expected 3 columns, got ${batch.columns.length}",
    )
    val keys = int32At(batch, 0)
    val left = int32At(batch, 1)
    val right = int32At(batch, 2)
    require(left.length == keys.length, "key and left value columns differ in length")
    require(right.length == keys.length, "key and right value columns differ in length")
    (keys, left, right)

  private def int32At(batch: ColumnBatch, ordinal: Int): Int32Column =
    ColumnKernel.asInt32(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private def boolAt(batch: ColumnBatch, ordinal: Int): BoolColumn =
    ColumnKernel.columnAt(batch, ordinal) match
      case bool: BoolColumn => bool
      case other =>
        throw new IllegalArgumentException(
          s"column $ordinal is ${other.logicalType}, expected Bool",
        )

  /**
   * One gather of every column. Arrays are sized to the morsel. A set source bit copies the value
   * and the bit. A clear source bit stores zero and leaves the bit clear.
   */
  private def publish(
      batch: ColumnBatch,
      index: Array[Int],
      count: Int,
      capacity: Int,
  ): ColumnBatch =
    val columns = Vector.tabulate(batch.columns.length) { ordinal =>
      gather(ColumnKernel.columnAt(batch, ordinal), index, count, capacity)
    }
    new ColumnBatch(batch.schema, columns, count)

  private def gather(column: Column, index: Array[Int], count: Int, capacity: Int): Column =
    column match
      case int32: Int32Column => gatherInt32(int32, index, count, capacity)
      case int64: Int64Column => gatherInt64(int64, index, count, capacity)
      case float64: Float64Column => gatherFloat64(float64, index, count, capacity)
      case bool: BoolColumn => gatherBool(bool, index, count, capacity)
      case utf8: DictUtf8Column => gatherUtf8(utf8, index, count, capacity)

  private def gatherInt32(
      column: Int32Column,
      index: Array[Int],
      count: Int,
      capacity: Int,
  ): Int32Column =
    val in = column.values
    val srcWords = column.validity.words
    val out = new Array[Int](capacity)
    val dstWords = Validity.allocate(capacity)
    var slot = 0
    while (slot < count) {
      val row = index(slot)
      if (Validity.isSet(srcWords, row)) then
        out(slot) = in(row)
        Validity.setBit(dstWords, slot)
      else out(slot) = 0
      slot += 1
    }
    Int32Column.of(out, Validity.fromWords(dstWords, capacity), count)

  private def gatherInt64(
      column: Int64Column,
      index: Array[Int],
      count: Int,
      capacity: Int,
  ): Int64Column =
    val in = column.values
    val srcWords = column.validity.words
    val out = new Array[Long](capacity)
    val dstWords = Validity.allocate(capacity)
    var slot = 0
    while (slot < count) {
      val row = index(slot)
      if (Validity.isSet(srcWords, row)) then
        out(slot) = in(row)
        Validity.setBit(dstWords, slot)
      else out(slot) = 0L
      slot += 1
    }
    Int64Column.of(out, Validity.fromWords(dstWords, capacity), count)

  private def gatherFloat64(
      column: Float64Column,
      index: Array[Int],
      count: Int,
      capacity: Int,
  ): Float64Column =
    val in = column.values
    val srcWords = column.validity.words
    val out = new Array[Double](capacity)
    val dstWords = Validity.allocate(capacity)
    var slot = 0
    while (slot < count) {
      val row = index(slot)
      if (Validity.isSet(srcWords, row)) then
        out(slot) = in(row)
        Validity.setBit(dstWords, slot)
      else out(slot) = 0.0
      slot += 1
    }
    Float64Column.of(out, Validity.fromWords(dstWords, capacity), count)

  private def gatherBool(
      column: BoolColumn,
      index: Array[Int],
      count: Int,
      capacity: Int,
  ): BoolColumn =
    val in = column.values
    val srcWords = column.validity.words
    val out = new Array[Byte](capacity)
    val dstWords = Validity.allocate(capacity)
    var slot = 0
    while (slot < count) {
      val row = index(slot)
      if (Validity.isSet(srcWords, row)) then
        out(slot) = if (in(row) != 0) 1.toByte else 0.toByte
        Validity.setBit(dstWords, slot)
      else out(slot) = 0.toByte
      slot += 1
    }
    BoolColumn.of(out, Validity.fromWords(dstWords, capacity), count)

  private def gatherUtf8(
      column: DictUtf8Column,
      index: Array[Int],
      count: Int,
      capacity: Int,
  ): DictUtf8Column =
    val in = column.codes
    val srcWords = column.validity.words
    val out = new Array[Int](capacity)
    val dstWords = Validity.allocate(capacity)
    var slot = 0
    while (slot < count) {
      val row = index(slot)
      if (Validity.isSet(srcWords, row)) then
        out(slot) = in(row)
        Validity.setBit(dstWords, slot)
      else out(slot) = 0
      slot += 1
    }
    DictUtf8Column.of(column.dictionary, out, Validity.fromWords(dstWords, capacity), count)
