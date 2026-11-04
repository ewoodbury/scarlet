package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.ColumnarFallback

import ColumnarInterpreter.Columns

/** Concatenates the partitions of two same-kind sides. A mismatched pair is a decline. */
private[execution] object ColumnsConcat:

  def apply(left: Columns, right: Columns): Columns = (left, right) match
    case (Columns.Ints(a), Columns.Ints(b)) => Columns.Ints(a ++ b)
    case (Columns.Longs(a), Columns.Longs(b)) => Columns.Longs(a ++ b)
    case (Columns.Floats(a), Columns.Floats(b)) => Columns.Floats(a ++ b)
    case (Columns.Pairs(a), Columns.Pairs(b)) => Columns.Pairs(a ++ b)
    case (Columns.LongPairs(a), Columns.LongPairs(b)) => Columns.LongPairs(a ++ b)
    case (Columns.Triples(a), Columns.Triples(b)) => Columns.Triples(a ++ b)
    case (Columns.LongTriples(a), Columns.LongTriples(b)) => Columns.LongTriples(a ++ b)
    case (Columns.Strings(a), Columns.Strings(b)) => Columns.Strings(a ++ b)
    case (Columns.StringPairs(a), Columns.StringPairs(b)) => Columns.StringPairs(a ++ b)
    case (Columns.Bools(a), Columns.Bools(b)) => Columns.Bools(a ++ b)
    case (Columns.Outer(a, side), Columns.Outer(b, same)) if side == same =>
      Columns.Outer(a ++ b, side)
    case _ =>
      throw new com.ewoodbury.scarlet.columnar.ColumnarFallback(
        "union sides have different columns",
      )
