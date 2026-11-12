package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.Partition

/**
 * Encodes source partitions into column batches.
 *
 * Named erasure boundary, shared with [[ColumnarInterpreter]]. Classification already selected the
 * element type. An owned array holder is copied. Any other partition is cast here.
 */
private[execution] object SourceEncode:

  private[execution] def encodeFloats(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.DoubleArrays

    partitions.map { partition =>
      partition.data match
        case owned: DoubleArrays => BatchCodec.copiedFloat64s(owned.values)
        case other =>
          // Named erasure boundary. Classification already selected double elements.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[Double]]
          BatchCodec.float64s(rows)
    }.toVector

  private[execution] def encodeLongs(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.LongArrays

    partitions.map { partition =>
      partition.data match
        case owned: LongArrays => BatchCodec.copiedLongs(owned.values)
        case other =>
          // Named erasure boundary. Classification already selected long elements.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[Long]]
          BatchCodec.longs(rows)
    }.toVector

  private[execution] def encodeBools(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.BoolArrays

    partitions.map { partition =>
      partition.data match
        case owned: BoolArrays => BatchCodec.copiedBools(owned.values)
        case other =>
          // Named erasure boundary. Classification already selected boolean elements.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[Boolean]]
          BatchCodec.bools(rows)
    }.toVector

  private[execution] def encodeInts(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.IntArrays

    partitions.map { partition =>
      partition.data match
        case owned: IntArrays => BatchCodec.copiedInts(owned.values)
        case other =>
          // Named erasure boundary. Classification already selected int elements.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[Int]]
          BatchCodec.ints(rows)
    }.toVector

  /**
   * Encode the ordinals [[ColumnUse]] demanded. `{0}` copies keys and `{1}` copies values, each as
   * one `Int32` column at index 0. Any other set, including `{0, 1}`, keeps both columns. A boxed
   * partition keeps a null component absent and does not call `primitiveEncode`. An owned array
   * has no nulls and is copied, one `primitiveEncode` note per partition. A singleton demand does
   * not read the other component.
   */
  private[execution] def encodePairs(
      partitions: Seq[Partition[_]],
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    if (ordinals.size == 1 && ordinals.contains(0)) then encodeOneSide(partitions, key = true)
    else if (ordinals.size == 1 && ordinals.contains(1)) then
      encodeOneSide(partitions, key = false)
    else encodePairPartitions(partitions, rows => BatchCodec.intPairsPreservingAbsence(rows))

  private def encodeOneSide(partitions: Seq[Partition[_]], key: Boolean): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.IntPairArrays

    partitions.map { partition =>
      partition.data match
        case pairs: IntPairArrays =>
          val column = if (key) then pairs.keys else pairs.values
          BatchCodec.copiedInts(column)
        case other =>
          // Named erasure boundary. Classification already selected `(Int, Int)` elements.
          // `Product` keeps the read on `productElement`, so a null stays absent and the other
          // component is not read.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[Product]]
          val ordinal = if (key) then 0 else 1
          IntPairEncode.side(rows, ordinal)
    }.toVector

  /**
   * Encode the ordinals [[ColumnUse]] demanded. `{0}` copies keys and `{1}` copies values, each as
   * one `Int64` column at index 0. Any other set, including `{0, 1}`, keeps both columns. A boxed
   * one-side encode keeps a null and does not read the other component. Sort, reduce, and join
   * demand both columns, so they still encode both.
   */
  private[execution] def encodeLongPairs(
      partitions: Seq[Partition[_]],
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    if (ordinals.size == 1 && ordinals.contains(0)) then encodeLongSide(partitions, key = true)
    else if (ordinals.size == 1 && ordinals.contains(1)) then
      encodeLongSide(partitions, key = false)
    else encodeLongPairPartitions(partitions)

  private def encodeLongSide(partitions: Seq[Partition[_]], key: Boolean): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.LongPairArrays

    partitions.map { partition =>
      partition.data match
        case pairs: LongPairArrays =>
          val column = if (key) then pairs.keys else pairs.values
          BatchCodec.copiedLongs(column)
        case other =>
          // Named erasure boundary. Classification already selected long-pair elements.
          // `Product` keeps the read on `productElement`, so the other component is not cast.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[Product]]
          val ordinal = if (key) then 0 else 1
          BatchCodec.longPairSide(rows, ordinal)
    }.toVector

  private def encodeLongPairPartitions(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.LongPairArrays

    partitions.map { partition =>
      partition.data match
        case pairs: LongPairArrays => BatchCodec.copiedLongPairs(pairs.keys, pairs.values)
        case other =>
          // Named erasure boundary. Classification already selected `(Long, Long)` elements.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[(Long, Long)]]
          BatchCodec.longPairs(rows)
    }.toVector

  /**
   * Pair encode for grouped sum. Both columns are kept, including when a caller demanded one
   * ordinal. [[BatchCodec.intPairsPreservingAbsence]] keeps a null component absent. An owned pair
   * array has no nulls and is copied. The normal boxed scan uses that same encoder.
   */
  private[execution] def encodeGroupedPairs(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    encodePairPartitions(partitions, rows => BatchCodec.intPairsPreservingAbsence(rows))

  private def encodePairPartitions(
      partitions: Seq[Partition[_]],
      encodeBoxed: Iterable[(Int, Int)] => ColumnBatch,
  ): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.IntPairArrays

    partitions.map { partition =>
      partition.data match
        case pairs: IntPairArrays => BatchCodec.copiedIntPairs(pairs.keys, pairs.values)
        case other =>
          // Named erasure boundary. Classification already selected `(Int, Int)` elements.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[(Int, Int)]]
          encodeBoxed(rows)
    }.toVector

  /**
   * An owned [[com.ewoodbury.scarlet.core.StringArrays]] has no nulls. It is dictionary-coded with
   * [[BatchCodec.allValidStrings]]. Any other partition stays on [[BatchCodec.strings]], which
   * still packs a presence bitmap.
   */
  private[execution] def encodeStrings(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.StringArrays

    partitions.map { partition =>
      partition.data match
        case owned: StringArrays => BatchCodec.allValidStrings(owned.values)
        case other =>
          // Named erasure boundary. Classification already selected string elements.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[String]]
          BatchCodec.strings(rows)
    }.toVector
