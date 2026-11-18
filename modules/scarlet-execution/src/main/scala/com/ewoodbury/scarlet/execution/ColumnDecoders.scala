package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{BatchCodec, ColumnBatch, RankSplitTrace}

/**
 * Decoders for a finished columnar batch.
 *
 * A long key or value keeps a clear bit. `keys` of one int [[PhysicalOp.MapKey]] does too. The
 * stored payload of that null key is `0`, and [[BatchCodec.decodeInts]] would publish the zero.
 * Every other int column rejects a clear bit. Int-pair and triple choices are the ones
 * [[ColumnarInterpreter]] used before this file existed.
 */
private[execution] object ColumnDecoders:

  def ints(op: PhysicalOp): (ColumnBatch, Int) => Seq[Int] =
    if (mappedIntKey(op)) then BatchCodec.decodeIntsPreservingAbsence
    else (batch, rows) => BatchCodec.decodeInts(batch, rows)

  /** A key or value column of a long pair keeps a null. Every other long column rejects one. */
  def longs(op: PhysicalOp): (ColumnBatch, Int) => Seq[Long] =
    if (absentLongColumn(op)) then BatchCodec.decodeLongsPreservingAbsence
    else (batch, rows) => BatchCodec.decodeLongs(batch, rows)

  /**
   * Int-pair decoder for `op`. [[PhysicalOp.SortByLongKey]] and [[PhysicalOp.SortByStringKey]] are
   * their own arms. Those decoders do not call the returned function, and these arms must not fall
   * through to [[BatchCodec.decodeIntPairs]], which rejects a clear bit. [[MapIntKey.keepsNull]]
   * and an int-pair [[PhysicalOp.Project]] keep a null key or value, so they use the
   * absence-preserving decoder. A triple [[PhysicalOp.Project]] does not: its kind is not
   * [[PhysicalOp.Kind.Pairs]], and [[triples]] is the arm that runs.
   */
  def intPairs(op: PhysicalOp): (ColumnBatch, Int) => Seq[(Int, Int)] =
    op match
      case kept if MapIntKey.keepsNull(kept) =>
        (batch, rows) => BatchCodec.decodeIntPairsPreservingAbsence(batch, rows)
      case project: PhysicalOp.Project if intPairProject(project) =>
        (batch, rows) => BatchCodec.decodeIntPairsPreservingAbsence(batch, rows)
      case _: PhysicalOp.SortByIntKey | _: PhysicalOp.SortByIntValueDesc =>
        (batch, rows) =>
          val started = System.nanoTime()
          val decoded = BatchCodec.decodeIntPairsPreservingAbsence(batch, rows)
          RankSplitTrace.addOutput(System.nanoTime() - started)
          decoded
      case _: PhysicalOp.SortByLongKey =>
        (_, _) =>
          throw new IllegalStateException(
            "SortByLongKey is decoded as long pairs, not int pairs",
          )
      case _: PhysicalOp.SortByStringKey =>
        (_, _) =>
          throw new IllegalStateException(
            "SortByStringKey is decoded as string pairs, not int pairs",
          )
      case _ => (batch, rows) => BatchCodec.decodeIntPairs(batch, rows)

  /**
   * An int inner join or a triple map may keep a null component. A filter or other triple op
   * rejects a clear bit. A whole-row filter drops that row before decode. Outer joins are
   * [[ColumnarInterpreter.Columns.Outer]] and do not use this decoder.
   */
  def triples(op: PhysicalOp): (ColumnBatch, Int) => Seq[(Int, (Int, Int))] =
    op match
      case _: PhysicalOp.Project | _: PhysicalOp.HashJoin | _: PhysicalOp.BroadcastJoin |
          _: PhysicalOp.SortMergeJoin =>
        BatchCodec.decodeIntTriplesPreservingAbsence
      case _ => (batch, rows) => BatchCodec.decodeIntTriples(batch, rows)

  /** `keys` of one int key map. Ordinal 0 is the mapped key, nulls included. */
  private def mappedIntKey(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Column(_: PhysicalOp.MapKey, 0) => true
    case _ => false

  private def absentLongColumn(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Column(child, _) =>
      child.kind match
        case PhysicalOp.Kind.LongPairs => true
        case _ => false
    case _ => false

  /** An int-pair project. A triple project stays on [[triples]]. */
  private def intPairProject(op: PhysicalOp.Project): Boolean = op.kind match
    case PhysicalOp.Kind.Pairs => true
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Triples | PhysicalOp.Kind.Strings | PhysicalOp.Kind.Bools |
        PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.LongCounts | PhysicalOp.Kind.LongTriples |
        PhysicalOp.Kind.StringPairs | PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.GroupedInts |
        PhysicalOp.Kind.GroupedStrings | PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter |
        PhysicalOp.Kind.StringOuter | PhysicalOp.Kind.CountSums | PhysicalOp.Kind.IntLongSums =>
      false
