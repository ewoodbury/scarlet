package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.{ColumnField, ColumnKind}

/**
 * Exact distinct count of one int, long, or string column.
 *
 * Nulls are skipped. An empty string counts. `"é"` and `"e\u0301"` stay distinct because the set
 * stores the string, not the dictionary code. Empty input, and a column of only nulls, count as
 * zero. The grouped form is a second pass over the same batches: the group kernel is unchanged,
 * and each output row gains the size of that key's set. There is no spill.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
object WideDistinct:

  def count(batches: Seq[ColumnBatch], ordinal: Int, kind: ColumnKind): Long =
    kind match
      case ColumnKind.Int32 | ColumnKind.Int64 =>
        val seen = new LongSet
        batches.foreach(batch => addLongs(seen, batch, ordinal, kind))
        seen.count
      case ColumnKind.Utf8 =>
        val seen = new StringSet
        batches.foreach(batch => addTexts(seen, batch, ordinal))
        seen.count
      case other =>
        throw new IllegalArgumentException(s"distinct of $other is not supported")

  /**
   * Appends the distinct count of `distinct` onto a finished group batch. Keys in that batch are
   * columns 0 and, when `key2` is set, 1. Input ordinals are the source columns.
   */
  def attach(
      grouped: ColumnBatch,
      inputs: Seq[ColumnBatch],
      fields: Vector[ColumnField],
      key: Int,
      key2: Option[Int],
      distinct: Int,
  ): ColumnBatch =
    val kind = fields(distinct).kind
    val groups = new Groups(fields(key).kind, key2.map(fields(_).kind), kind)
    inputs.foreach(batch => groups.observe(batch, key, key2, distinct))
    val counts = groups.counts(grouped)
    append(grouped, counts)

  private def addLongs(seen: LongSet, batch: ColumnBatch, ordinal: Int, kind: ColumnKind): Unit =
    val column = batch.columns(ordinal)
    var row = 0
    while row < batch.length do
      if column.isValid(row) then
        val value = kind match
          case ColumnKind.Int32 =>
            column match
              case ints: Int32Column => ints.values(row).toLong
              case other =>
                throw new IllegalStateException(
                  s"column $ordinal is ${other.logicalType}, expected Int32",
                )
          case _ =>
            column match
              case longs: Int64Column => longs.values(row)
              case other =>
                throw new IllegalStateException(
                  s"column $ordinal is ${other.logicalType}, expected Int64",
                )
        seen.add(value)
      row += 1

  private def addTexts(seen: StringSet, batch: ColumnBatch, ordinal: Int): Unit =
    val column = textColumn(batch, ordinal)
    var row = 0
    while row < batch.length do
      if column.isValid(row) then seen.add(column.dictionary(column.codes(row)))
      row += 1

  private def append(batch: ColumnBatch, values: Array[Long]): ColumnBatch =
    val column = Int64Column.of(values, Validity.allValid(batch.length), batch.length)
    val columns = batch.columns :+ column
    new ColumnBatch(batch.schema :+ column.logicalType, columns, batch.length)

  private def textColumn(batch: ColumnBatch, ordinal: Int): DictUtf8Column =
    batch.columns(ordinal) match
      case column: DictUtf8Column => column
      case other =>
        throw new IllegalStateException(s"column $ordinal is ${other.logicalType}, expected Utf8")

  private def longOf(batch: ColumnBatch, ordinal: Int, row: Int): Long =
    batch.columns(ordinal) match
      case column: Int32Column => column.values(row).toLong
      case column: Int64Column => column.values(row)
      case other =>
        throw new IllegalStateException(s"column $ordinal is ${other.logicalType}, expected a key")

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class LongSet:
    private var cap = 16
    private var size = 0
    private var keys = new Array[Long](cap)
    private var used = new Array[Boolean](cap)

    def count: Long = size.toLong

    def add(value: Long): Unit =
      if (size + 1) * 10 > cap * 7 then grow()
      place(value)

    private def place(value: Long): Unit =
      var index = mix(value) & (cap - 1)
      var steps = 0
      while steps < cap && used(index) && keys(index) != value do
        index = (index + 1) & (cap - 1)
        steps += 1
      if steps >= cap then throw new IllegalStateException("distinct table is full")
      if !used(index) then
        used(index) = true
        keys(index) = value
        size += 1

    private def grow(): Unit =
      val oldCap = cap
      val oldKeys = keys
      val oldUsed = used
      cap *= 2
      keys = new Array[Long](cap)
      used = new Array[Boolean](cap)
      size = 0
      var row = 0
      while row < oldCap do
        if oldUsed(row) then place(oldKeys(row))
        row += 1

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  private final class StringSet:
    private var cap = 16
    private var size = 0
    private var keys = new Array[String](cap)
    private var used = new Array[Boolean](cap)

    def count: Long = size.toLong

    def add(value: String): Unit =
      if (size + 1) * 10 > cap * 7 then grow()
      place(value)

    private def place(value: String): Unit =
      var index = mixText(value) & (cap - 1)
      var steps = 0
      while steps < cap && used(index) && !keys(index).equals(value) do
        index = (index + 1) & (cap - 1)
        steps += 1
      if steps >= cap then throw new IllegalStateException("distinct table is full")
      if !used(index) then
        used(index) = true
        keys(index) = value
        size += 1

    private def grow(): Unit =
      val oldCap = cap
      val oldKeys = keys
      val oldUsed = used
      cap *= 2
      keys = new Array[String](cap)
      used = new Array[Boolean](cap)
      size = 0
      var row = 0
      while row < oldCap do
        if oldUsed(row) then place(oldKeys(row))
        row += 1

  /** One set per group key. A missing distinct value leaves the key's count at zero. */
  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  private final class Groups(
      leftKind: ColumnKind,
      rightKind: Option[ColumnKind],
      distinctKind: ColumnKind,
  ):
    private val leftText = leftKind == ColumnKind.Utf8
    private val two = rightKind.isDefined
    private val rightText = rightKind.contains(ColumnKind.Utf8)
    private val textual = distinctKind == ColumnKind.Utf8
    private var cap = 16
    private var size = 0
    private var used = new Array[Boolean](cap)
    private var leftNum = new Array[Long](cap)
    private var rightNum = new Array[Long](cap)
    private var leftChars = new Array[String](cap)
    private var rightChars = new Array[String](cap)
    private var longs = new Array[LongSet](cap)
    private var texts = new Array[StringSet](cap)

    def observe(
        batch: ColumnBatch,
        key: Int,
        key2: Option[Int],
        distinct: Int,
    ): Unit =
      var row = 0
      while row < batch.length do
        if keysPresent(batch, key, key2, row) then
          val slot = slotOf(batch, key, key2, row)
          val column = batch.columns(distinct)
          if column.isValid(row) then addValue(slot, batch, distinct, row)
        row += 1

    def counts(grouped: ColumnBatch): Array[Long] =
      val rows = grouped.length
      val out = new Array[Long](rows)
      var row = 0
      while row < rows do
        val slot = findGrouped(grouped, row)
        out(row) = if slot < 0 then 0L else sizeAt(slot)
        row += 1
      out

    private def addValue(slot: Int, batch: ColumnBatch, distinct: Int, row: Int): Unit =
      if textual then
        texts(slot).add(
          textColumn(batch, distinct).dictionary(textColumn(batch, distinct).codes(row)),
        )
      else
        val value = distinctKind match
          case ColumnKind.Int32 =>
            batch.columns(distinct) match
              case column: Int32Column => column.values(row).toLong
              case other =>
                throw new IllegalStateException(
                  s"column $distinct is ${other.logicalType}, expected Int32",
                )
          case ColumnKind.Int64 =>
            batch.columns(distinct) match
              case column: Int64Column => column.values(row)
              case other =>
                throw new IllegalStateException(
                  s"column $distinct is ${other.logicalType}, expected Int64",
                )
          case other =>
            throw new IllegalArgumentException(s"distinct of $other is not supported")
        longs(slot).add(value)

    private def sizeAt(slot: Int): Long =
      if textual then texts(slot).count else longs(slot).count

    private def keysPresent(
        batch: ColumnBatch,
        key: Int,
        key2: Option[Int],
        row: Int,
    ): Boolean =
      batch.columns(key).isValid(row) && key2.forall(second => batch.columns(second).isValid(row))

    private def slotOf(batch: ColumnBatch, key: Int, key2: Option[Int], row: Int): Int =
      if (size + 1) * 10 > cap * 7 then grow()
      place(
        readNum(batch, key, row, leftText),
        readText(batch, key, row, leftText),
        key2.map(second => readNum(batch, second, row, rightText)),
        key2.map(second => readText(batch, second, row, rightText)),
        create = true,
      )

    private def findGrouped(grouped: ColumnBatch, row: Int): Int =
      if size == 0 then -1
      else
        val rightNumber = if two then Some(readNum(grouped, 1, row, rightText)) else None
        val rightString = if two then Some(readText(grouped, 1, row, rightText)) else None
        locate(
          readNum(grouped, 0, row, leftText),
          readText(grouped, 0, row, leftText),
          rightNumber,
          rightString,
          create = false,
        )

    private def place(
        leftNumber: Long,
        leftString: String,
        rightNumber: Option[Long],
        rightString: Option[String],
        create: Boolean,
    ): Int =
      locate(leftNumber, leftString, rightNumber, rightString, create)

    private def locate(
        leftNumber: Long,
        leftString: String,
        rightNumber: Option[Long],
        rightString: Option[String],
        create: Boolean,
    ): Int =
      var index = mixKey(leftNumber, leftString, rightNumber, rightString) & (cap - 1)
      var steps = 0
      while steps < cap && used(index) && !same(
          index,
          leftNumber,
          leftString,
          rightNumber,
          rightString,
        )
      do
        index = (index + 1) & (cap - 1)
        steps += 1
      if steps >= cap then
        if create then throw new IllegalStateException("distinct table is full")
        else -1
      else if used(index) then index
      else if !create then -1
      else
        used(index) = true
        leftNum(index) = leftNumber
        leftChars(index) = leftString
        rightNumber.foreach(value => rightNum(index) = value)
        rightString.foreach(value => rightChars(index) = value)
        if textual then texts(index) = new StringSet else longs(index) = new LongSet
        size += 1
        index

    private def same(
        index: Int,
        leftNumber: Long,
        leftString: String,
        rightNumber: Option[Long],
        rightString: Option[String],
    ): Boolean =
      val leftMatches =
        if leftText then leftChars(index).equals(leftString) else leftNum(index) == leftNumber
      val rightMatches =
        if !two then true
        else if rightText then rightChars(index).equals(rightString.getOrElse(""))
        else rightNum(index) == rightNumber.getOrElse(0L)
      leftMatches && rightMatches

    private def grow(): Unit =
      val oldCap = cap
      val oldUsed = used
      val oldLeftNum = leftNum
      val oldRightNum = rightNum
      val oldLeftChars = leftChars
      val oldRightChars = rightChars
      val oldLongs = longs
      val oldTexts = texts
      cap *= 2
      size = 0
      used = new Array[Boolean](cap)
      leftNum = new Array[Long](cap)
      rightNum = new Array[Long](cap)
      leftChars = new Array[String](cap)
      rightChars = new Array[String](cap)
      longs = new Array[LongSet](cap)
      texts = new Array[StringSet](cap)
      var row = 0
      while row < oldCap do
        if oldUsed(row) then
          val slot = place(
            oldLeftNum(row),
            oldLeftChars(row),
            if two then Some(oldRightNum(row)) else None,
            if two then Some(oldRightChars(row)) else None,
            create = true,
          )
          if textual then texts(slot) = oldTexts(row) else longs(slot) = oldLongs(row)
        row += 1

    private def readNum(batch: ColumnBatch, ordinal: Int, row: Int, text: Boolean): Long =
      if text then 0L else longOf(batch, ordinal, row)

    private def readText(batch: ColumnBatch, ordinal: Int, row: Int, text: Boolean): String =
      if text then textColumn(batch, ordinal).dictionary(textColumn(batch, ordinal).codes(row))
      else ""

    private def mixKey(
        leftNumber: Long,
        leftString: String,
        rightNumber: Option[Long],
        rightString: Option[String],
    ): Int =
      val left = if leftText then mixText(leftString) else mix(leftNumber)
      val right =
        if !two then 0
        else if rightText then mixText(rightString.getOrElse(""))
        else mix(rightNumber.getOrElse(0L))
      left ^ Integer.rotateLeft(right, 13)

  private def mix(key: Long): Int =
    val mixed = key * 0x9e3779b97f4a7c15L
    val folded = mixed ^ (mixed >>> 32)
    folded.toInt

  private def mixText(text: String): Int =
    var hash = 0x811c9dc5
    var index = 0
    while index < text.length do
      hash ^= text.charAt(index).toInt
      hash *= 0x01000193
      index += 1
    hash
