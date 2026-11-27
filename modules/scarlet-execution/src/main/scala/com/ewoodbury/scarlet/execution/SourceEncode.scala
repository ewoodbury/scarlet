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

  /**
   * Long scan under `distinct`. An owned array has no null. A boxed cell keeps a null as a clear
   * bit, so `0L` stays a value. Other operators still use [[encodeLongs]], which unboxes.
   */
  private[execution] def encodeNullableLongs(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.LongArrays

    partitions.map { partition =>
      partition.data match
        case owned: LongArrays => BatchCodec.copiedLongs(owned.values)
        case other => nullableLongs(other)
    }.toVector

  /**
   * Double scan under `distinct`. An owned array has no null. A boxed cell keeps a null as a clear
   * bit. `-0.0` stays `-0.0` in the column; the kernel canonicalises it. Other operators still use
   * [[encodeFloats]], which unboxes.
   */
  private[execution] def encodeNullableDoubles(
      partitions: Seq[Partition[_]],
  ): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.DoubleArrays

    partitions.map { partition =>
      partition.data match
        case owned: DoubleArrays => BatchCodec.copiedFloat64s(owned.values)
        case other => nullableDoubles(other)
    }.toVector

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullableLongs(values: Iterable[Any]): ColumnBatch =
    val cells = Vector.newBuilder[Option[Long]]
    values.iterator.foreach { cell =>
      cell match
        case null => cells += None
        case value: Long => cells += Some(value)
        case other =>
          val found = other.asInstanceOf[AnyRef].getClass.getName
          throw new ColumnarFallback(s"columnar long expected a Long, found $found")
    }
    val built = cells.result()
    if (built.forall(_.isDefined)) then BatchCodec.longs(built.flatten)
    else BatchCodec.nullableLongs(built)

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullableDoubles(values: Iterable[Any]): ColumnBatch =
    val cells = Vector.newBuilder[Option[Double]]
    values.iterator.foreach { cell =>
      cell match
        case null => cells += None
        case value: Double => cells += Some(value)
        case other =>
          val found = other.asInstanceOf[AnyRef].getClass.getName
          throw new ColumnarFallback(s"columnar double expected a Double, found $found")
    }
    val built = cells.result()
    if (built.forall(_.isDefined)) then BatchCodec.float64s(built.flatten)
    else BatchCodec.nullableFloat64s(built)

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
   * one-side encode keeps a null and does not read the other component. Sort and join demand both
   * columns, so they still copy both. A folded long `reduceByKey` of owned arrays uses
   * [[shareOwnedLongPairs]] and does not call this.
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

  /**
   * One batch per partition, in partition order. Two or more partitions run on the morsel pool. A
   * single partition, and a pool width of one, stay on the caller. The copy itself does not depend
   * on the pool; scheduling it is what keeps the scan off one thread when the action is wide.
   */
  private def encodeLongPairPartitions(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    val parts = partitions.toVector
    val workers = ColumnarPlanner.parallelism
    if (parts.length <= 1 || workers <= 1) then parts.map(encodeOneLongPartition)
    else MorselScheduler.map(parts, workers)((_, partition) => encodeOneLongPartition(partition))

  private def encodeOneLongPartition(partition: Partition[_]): ColumnBatch =
    import com.ewoodbury.scarlet.core.LongPairArrays

    partition.data match
      case pairs: LongPairArrays => BatchCodec.copiedLongPairs(pairs.keys, pairs.values)
      case other =>
        // Named erasure boundary. Classification already selected `(Long, Long)` elements.
        @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
        val rows = other.asInstanceOf[Iterable[(Long, Long)]]
        BatchCodec.longPairs(rows)

  /**
   * One batch per owned long-pair partition, in partition order, sharing the partition arrays.
   *
   * `None` when any partition is boxed. Those rows can hold a null, and the encoder is what
   * records it. An owned array has no nulls. The batch borrows the arrays for the fold and is not
   * a published result: the reduce writes new output arrays and does not mutate these. Two
   * half-million-row copies are larger than a G1 region, and scheduling them on the pool made the
   * four-thread scan slower than the copy.
   */
  private[execution] def shareOwnedLongPairs(
      partitions: Seq[Partition[_]],
  ): Option[Vector[ColumnBatch]] =
    import com.ewoodbury.scarlet.core.LongPairArrays

    def shareOne(pairs: LongPairArrays): ColumnBatch =
      val rows = pairs.keys.length
      new ColumnBatch(
        Vector(LogicalType.Int64, LogicalType.Int64),
        Vector(
          Int64Column.of(pairs.keys, Validity.allValid(rows), rows),
          Int64Column.of(pairs.values, Validity.allValid(rows), rows),
        ),
        rows,
      )

    val shared = partitions.foldLeft(Option(List.empty[ColumnBatch])) { (acc, partition) =>
      (acc, partition.data) match
        case (Some(built), pairs: LongPairArrays) => Some(shareOne(pairs) :: built)
        case _ => None
    }
    shared.map(built => built.reverse.toVector)

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
