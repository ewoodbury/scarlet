package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.Partition

import ColumnarInterpreter.Columns

/**
 * Collect and `take` decode for a lowered batch.
 *
 * [[ColumnarInterpreter]] keeps the route. This object only reads the columns the route already
 * chose. A long join decodes to `(Long, (Long, Long))` and a string join to
 * `(String, (Int, Int))`. [[ColumnarPlanner.runLowered]] casts that sequence to the plan's element
 * type.
 */
// Outer rows do not share one element type, so that decoder is Seq[Any].
@SuppressWarnings(Array("org.wartremover.warts.Any"))
private[execution] object ColumnDecode:

  def outer(batch: ColumnBatch, absent: AbsentSide, maxRows: Int): Seq[Any] =
    if (StringOuterDecode.isStringOuter(batch)) then
      StringOuterDecode.decode(batch, absent, maxRows)
    else LongOuterDecode.decode(batch, absent, maxRows)

  def partitions(
      columns: Columns,
      decodeInts: (ColumnBatch, Int) => Seq[Int],
      decodePairs: (ColumnBatch, Int) => Seq[(Int, Int)],
      decodeLongs: (ColumnBatch, Int) => Seq[Long],
      decodeTriples: (ColumnBatch, Int) => Seq[(Int, (Int, Int))],
      decodeFloats: (ColumnBatch, Int) => Seq[Double],
  ): Seq[Partition[_]] = columns match
    case Columns.Ints(parts) =>
      parts.map(batch => Partition(decodeInts(batch, batch.length)))
    case Columns.Longs(parts) =>
      parts.map(batch => Partition(decodeLongs(batch, batch.length)))
    case Columns.Floats(parts) =>
      parts.map(batch => Partition(decodeFloats(batch, batch.length)))
    case Columns.Pairs(parts) =>
      parts.map(batch => Partition(decodePairs(batch, batch.length)))
    case Columns.LongCounts(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeLongCounts(batch)))
    case Columns.CountSums(parts) =>
      parts.map(batch => Partition(CountSum.rows(batch, batch.length)))
    case Columns.LongCountSums(parts) =>
      parts.map(batch => Partition(LongCountSum.rows(batch, batch.length)))
    case Columns.StringCountSums(parts) =>
      parts.map(batch => Partition(StringCountSum.rows(batch, batch.length)))
    case Columns.LongPairs(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeLongPairs(batch)))
    case Columns.Triples(parts) =>
      parts.map(batch => Partition(decodeTriples(batch, batch.length)))
    case Columns.LongTriples(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeLongTriplesPreservingAbsence(batch)))
    case Columns.Outer(parts, absent) =>
      parts.map(batch => Partition(outer(batch, absent, batch.length)))
    case Columns.Strings(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeStrings(batch)))
    case Columns.StringPairs(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeStringPairs(batch)))
    case Columns.StringTriples(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeStringJoinKeepingNulls(batch, batch.length)))
    case Columns.Bools(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeBools(batch)))
    case Columns.GroupedInts(parts) =>
      parts.map(batch => Partition(batch.rows))
    case Columns.GroupedStrings(parts) =>
      parts.map(batch => Partition(batch.rows))
    case Columns.GroupedLongs(parts) =>
      parts.map(batch => Partition(batch.rows))
    case Columns.GroupedIntLongs(parts) =>
      parts.map(batch => Partition(batch.rows))
