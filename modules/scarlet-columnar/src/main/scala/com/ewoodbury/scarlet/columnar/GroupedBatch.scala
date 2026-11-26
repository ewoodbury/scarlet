package com.ewoodbury.scarlet.columnar

/**
 * One shuffle partition of int `groupByKey`.
 *
 * This is not a [[ColumnBatch]]. Keys are `groups` long, offsets are `groups + 1`, and values are
 * the input cardinality. `offsets(0) == 0`, offsets are monotone, and `offsets(groups)` is
 * `values.length`. Group `i` is `values[offsets(i) until offsets(i + 1))`. An empty bucket is `0`
 * groups, `offsets = Array(0)`, and an empty values array. `keys` is fully present.
 *
 * [[valuesFor]] is a view. A published array is not mutated, and the caller does not recycle it,
 * which is the same rule as [[Column]]. Holding one group pins the whole values array. The view is
 * an [[IndexedSeq]], so iteration is multi-pass and `equals` is structural.
 */
final case class GroupedIntBatch(keys: Int32Column, offsets: Array[Int], values: Array[Int]):
  require(
    offsets.length == keys.length + 1,
    s"offsets length ${offsets.length} is not groups + 1 (${keys.length + 1})",
  )
  require(offsets(0) == 0, s"offsets must start at 0, found ${offsets(0)}")
  require(
    offsets(keys.length) == values.length,
    s"offsets end ${offsets(keys.length)} does not match ${values.length} values",
  )
  require(monotonicOffsets, "offsets must be monotone")
  require(ColumnHash.fullyPresent(keys), "grouped keys must be fully present")

  def length: Int = keys.length

  def groups: Int = keys.length

  /** View of `values[offsets(group) until offsets(group + 1))`. Does not copy. */
  def valuesFor(group: Int): IndexedSeq[Int] =
    require(group >= 0 && group < groups, s"group $group outside 0 until $groups")
    new IntGroupView(values, offsets(group), offsets(group + 1))

  /** One `(key, values)` row per group, in key order. Each value side is [[valuesFor]]. */
  def rows: Seq[(Int, Iterable[Int])] =
    Seq.tabulate(groups)(group => (keys.values(group), valuesFor(group)))

  private def monotonicOffsets: Boolean =
    offsets.indices.drop(1).forall(index => offsets(index) >= offsets(index - 1))

object GroupedIntBatch:

  /** A fresh empty bucket. `0` groups, `offsets = Array(0)`, and no values. */
  def empty: GroupedIntBatch =
    GroupedIntBatch(
      Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
      Array(0),
      Array.emptyIntArray,
    )

/**
 * One group's values. `apply` and `length` read `values(start + i)`. The array is not copied.
 */
private final class IntGroupView(values: Array[Int], start: Int, end: Int) extends IndexedSeq[Int]:

  def length: Int = end - start

  def apply(index: Int): Int =
    if (index < 0 || index >= length) then
      throw new IndexOutOfBoundsException(s"index $index outside length $length")
    else values(start + index)

/**
 * One shuffle partition of string `groupByKey`.
 *
 * The shape matches [[GroupedIntBatch]]. Keys are a dictionary column `groups` long, offsets are
 * `groups + 1`, and values are the input cardinality. `offsets(0) == 0`, offsets are monotone, and
 * `offsets(groups)` is `values.length`. Group `i` is `values[offsets(i) until offsets(i + 1))`. An
 * empty bucket is `0` groups, `offsets = Array(0)`, an empty values array, and an empty
 * dictionary. `keys` is fully present. [[rows]] reads `keys.dictionary(keys.codes(group))`.
 *
 * [[valuesFor]] is the same view as [[GroupedIntBatch.valuesFor]]. A published array is not
 * mutated, and the caller does not recycle it. Holding one group pins the whole values array. The
 * view is an [[IndexedSeq]], so iteration is multi-pass and `equals` is structural.
 */
final case class GroupedStringBatch(keys: DictUtf8Column, offsets: Array[Int], values: Array[Int]):
  require(
    offsets.length == keys.length + 1,
    s"offsets length ${offsets.length} is not groups + 1 (${keys.length + 1})",
  )
  require(offsets(0) == 0, s"offsets must start at 0, found ${offsets(0)}")
  require(
    offsets(keys.length) == values.length,
    s"offsets end ${offsets(keys.length)} does not match ${values.length} values",
  )
  require(monotonicOffsets, "offsets must be monotone")
  require(ColumnHash.fullyPresent(keys), "grouped keys must be fully present")

  def length: Int = keys.length

  def groups: Int = keys.length

  /** View of `values[offsets(group) until offsets(group + 1))`. Does not copy. */
  def valuesFor(group: Int): IndexedSeq[Int] =
    require(group >= 0 && group < groups, s"group $group outside 0 until $groups")
    new IntGroupView(values, offsets(group), offsets(group + 1))

  /** One `(key, values)` row per group, in key order. Each value side is [[valuesFor]]. */
  def rows: Seq[(String, Iterable[Int])] =
    Seq.tabulate(groups)(group => (keys.dictionary(keys.codes(group)), valuesFor(group)))

  private def monotonicOffsets: Boolean =
    offsets.indices.drop(1).forall(index => offsets(index) >= offsets(index - 1))

object GroupedStringBatch:

  /**
   * A fresh empty bucket. `0` groups, `offsets = Array(0)`, no values, and an empty dictionary.
   */
  def empty: GroupedStringBatch =
    GroupedStringBatch(
      DictUtf8Column.of(Array.empty[String], Array.emptyIntArray, Validity.allValid(0), 0),
      Array(0),
      Array.emptyIntArray,
    )

/**
 * One shuffle partition of long `groupByKey`.
 *
 * The shape matches [[GroupedIntBatch]]. Keys are an `Int64` column `groups` long, offsets are
 * `groups + 1`, and values are the input cardinality. `offsets(0) == 0`, offsets are monotone, and
 * `offsets(groups)` is `values.length`. Group `i` is `values[offsets(i) until offsets(i + 1))`. An
 * empty bucket is `0` groups, `offsets = Array(0)`, and an empty values array. `keys` is fully
 * present. A key is the full `Long`.
 *
 * [[valuesFor]] is a view over `Array[Long]`. A published array is not mutated, and the caller
 * does not recycle it. Holding one group pins the whole values array. The view is an
 * [[IndexedSeq]], so iteration is multi-pass and `equals` is structural.
 */
final case class GroupedLongBatch(keys: Int64Column, offsets: Array[Int], values: Array[Long]):
  require(
    offsets.length == keys.length + 1,
    s"offsets length ${offsets.length} is not groups + 1 (${keys.length + 1})",
  )
  require(offsets(0) == 0, s"offsets must start at 0, found ${offsets(0)}")
  require(
    offsets(keys.length) == values.length,
    s"offsets end ${offsets(keys.length)} does not match ${values.length} values",
  )
  require(monotonicOffsets, "offsets must be monotone")
  require(ColumnHash.fullyPresent(keys), "grouped keys must be fully present")

  def length: Int = keys.length

  def groups: Int = keys.length

  /** View of `values[offsets(group) until offsets(group + 1))`. Does not copy. */
  def valuesFor(group: Int): IndexedSeq[Long] =
    require(group >= 0 && group < groups, s"group $group outside 0 until $groups")
    new LongGroupView(values, offsets(group), offsets(group + 1))

  /** One `(key, values)` row per group, in key order. Each value side is [[valuesFor]]. */
  def rows: Seq[(Long, Iterable[Long])] =
    Seq.tabulate(groups)(group => (keys.values(group), valuesFor(group)))

  private def monotonicOffsets: Boolean =
    offsets.indices.drop(1).forall(index => offsets(index) >= offsets(index - 1))

object GroupedLongBatch:

  /** A fresh empty bucket. `0` groups, `offsets = Array(0)`, and no values. */
  def empty: GroupedLongBatch =
    GroupedLongBatch(
      Int64Column.of(Array.emptyLongArray, Validity.allValid(0), 0),
      Array(0),
      Array.emptyLongArray,
    )

/**
 * One group's long values. `apply` and `length` read `values(start + i)`. The array is not copied.
 */
private final class LongGroupView(values: Array[Long], start: Int, end: Int)
    extends IndexedSeq[Long]:

  def length: Int = end - start

  def apply(index: Int): Long =
    if (index < 0 || index >= length) then
      throw new IndexOutOfBoundsException(s"index $index outside length $length")
    else values(start + index)

/**
 * One shuffle partition of `groupByKey` on int keys and long values.
 *
 * The shape matches [[GroupedIntBatch]]. Keys are an `Int32` column `groups` long, offsets are
 * `groups + 1`, and values are an `Int64` column of the input cardinality. A clear value bit is a
 * null `Long` in [[valuesFor]], not a dropped row. `keys` is fully present. An empty bucket is `0`
 * groups, `offsets = Array(0)`, and an empty value column.
 *
 * [[valuesFor]] is a view. A published column is not mutated. Holding one group pins the whole
 * value column. The view is an [[Iterable]] that re-reads the column, so iteration is multi-pass.
 * It is not an [[IndexedSeq]]: `apply` would return `Long` and turn a clear bit into `0L`.
 */
final case class GroupedIntLongBatch(
    keys: Int32Column,
    offsets: Array[Int],
    values: Int64Column,
):
  require(
    offsets.length == keys.length + 1,
    s"offsets length ${offsets.length} is not groups + 1 (${keys.length + 1})",
  )
  require(offsets(0) == 0, s"offsets must start at 0, found ${offsets(0)}")
  require(
    offsets(keys.length) == values.length,
    s"offsets end ${offsets(keys.length)} does not match ${values.length} values",
  )
  require(monotonicOffsets, "offsets must be monotone")
  require(ColumnHash.fullyPresent(keys), "grouped keys must be fully present")

  def length: Int = keys.length

  def groups: Int = keys.length

  /** View of the values in this group. A clear bit is a null `Long`. Does not copy. */
  def valuesFor(group: Int): Iterable[Long] =
    require(group >= 0 && group < groups, s"group $group outside 0 until $groups")
    new NullableLongGroupView(values, offsets(group), offsets(group + 1))

  /** One `(key, values)` row per group, in key order. Each value side is [[valuesFor]]. */
  def rows: Seq[(Int, Iterable[Long])] =
    Seq.tabulate(groups)(group => (keys.values(group), valuesFor(group)))

  private def monotonicOffsets: Boolean =
    offsets.indices.drop(1).forall(index => offsets(index) >= offsets(index - 1))

object GroupedIntLongBatch:

  /** A fresh empty bucket. `0` groups, `offsets = Array(0)`, and no values. */
  def empty: GroupedIntLongBatch =
    GroupedIntLongBatch(
      Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
      Array(0),
      Int64Column.of(Array.emptyLongArray, Validity.allValid(0), 0),
    )

/**
 * One group's long values. A clear bit is a null reference, not `0L`. The column is not copied.
 *
 * [[scala.collection.IndexedSeq]] cannot carry that null: `apply` returns `Long` and unboxes a
 * clear bit to `0L`. The iterator below is an `AnyRef` iterator so a cast to [[Iterable]] of
 * references still sees null. Reading the group as `Long` unboxes, which is the same rule as a row
 * `Iterable[Long]`.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Var",
  ),
)
private final class NullableLongGroupView(values: Int64Column, start: Int, end: Int)
    extends Iterable[Long]:

  /**
   * Named erasure boundary. `Iterator[Long].next` unboxes, so this iterator yields references and
   * the cast keeps a clear bit as null.
   */
  override def iterator: Iterator[Long] =
    val refs = new Iterator[AnyRef]:
      private var index: Int = start

      def hasNext: Boolean = index < end

      def next(): AnyRef =
        if (index >= end) then throw new NoSuchElementException("empty long group")
        else
          val row = index
          index += 1
          if (values.isValid(row)) then Long.box(values.values(row)) else null
    refs.asInstanceOf[Iterator[Long]]
