package com.ewoodbury.scarlet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.columnar.LogicalType
import com.ewoodbury.scarlet.core.Partition
import com.ewoodbury.scarlet.execution.schema.{
  ColumnLayout,
  ColumnType,
  Layout,
  NullPolicy,
  Output,
}

/**
 * [[Layout.known]] for constructed nodes. This does not call [[ColumnarPlanner.lower]].
 * [[PhysicalOp.schema]] stays the admission schema.
 */
class TestLayout extends AnyFlatSpec with Matchers:

  private val empty: Seq[Partition[_]] = Seq.empty

  private def scan(kind: PhysicalOp.Kind): PhysicalOp = PhysicalOp.Scan(kind, empty)

  private val ints: PhysicalOp = scan(PhysicalOp.Kind.Ints)
  private val longs: PhysicalOp = scan(PhysicalOp.Kind.Longs)
  private val floats: PhysicalOp = scan(PhysicalOp.Kind.Floats)
  private val bools: PhysicalOp = scan(PhysicalOp.Kind.Bools)
  private val strings: PhysicalOp = scan(PhysicalOp.Kind.Strings)
  private val pairs: PhysicalOp = scan(PhysicalOp.Kind.Pairs)
  private val longPairs: PhysicalOp = scan(PhysicalOp.Kind.LongPairs)
  private val stringPairs: PhysicalOp = scan(PhysicalOp.Kind.StringPairs)
  private val triples: PhysicalOp = scan(PhysicalOp.Kind.Triples)

  "Layout.known" should "report a scan from its kind" in {
    Layout.known(ints) shouldBe columns(LogicalType.Int32)
    Layout.known(longs) shouldBe columns(LogicalType.Int64)
    Layout.known(floats) shouldBe columns(LogicalType.Float64)
    Layout.known(bools) shouldBe columns(LogicalType.Bool)
    Layout.known(strings) shouldBe columns(LogicalType.Utf8Dict)
    Layout.known(pairs) shouldBe columns(LogicalType.Int32, LogicalType.Int32)
    Layout.known(longPairs) shouldBe columns(LogicalType.Int64, LogicalType.Int64)
    Layout.known(stringPairs) shouldBe columns(LogicalType.Utf8Dict, LogicalType.Int32)
  }

  it should "copy a filter and an exchange, and report kind elsewhere" in {
    val projected = PhysicalOp.Project(triples)
    val tripleColumns = columns(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32)
    Layout.known(PhysicalOp.Filter(projected, PhysicalOp.Slot.Key)) shouldBe Layout.AssumedIntLong
    Layout.known(PhysicalOp.Exchange(projected)) shouldBe Layout.AssumedIntLong
    Layout.known(PhysicalOp.Pipeline(projected, 2)) shouldBe tripleColumns
    Layout.known(PhysicalOp.Union(projected, longs)) shouldBe tripleColumns
    Layout.known(PhysicalOp.Union(longs, ints)) shouldBe Layout.known(longs)
    Layout.known(PhysicalOp.Dedupe(PhysicalOp.Exchange(projected))) shouldBe tripleColumns
    Layout.known(PhysicalOp.Dedupe(PhysicalOp.Exchange(strings))) shouldBe Layout.known(strings)
  }

  it should "assume int-long for a triple project and keep schema unchanged" in {
    val projected = PhysicalOp.Project(triples)
    Layout.known(projected) shouldBe Layout.AssumedIntLong
    projected.schema shouldBe PhysicalSchema.IntLong
    Layout.known(PhysicalOp.Project(pairs)) shouldBe Layout.known(pairs)
    PhysicalOp.Project(pairs).schema shouldBe PhysicalSchema.ofKind(PhysicalOp.Kind.Pairs)
  }

  it should "rewrite a map key to int and copy the value" in {
    Layout.known(PhysicalOp.MapKey(pairs)) shouldBe columns(LogicalType.Int32, LogicalType.Int32)
    val projected = PhysicalOp.MapKey(PhysicalOp.Project(triples))
    Layout.known(projected) shouldBe columns(LogicalType.Int32, LogicalType.Int64)
  }

  it should "copy the kind a map value already reports" in {
    Layout.known(PhysicalOp.MapValue(pairs)) shouldBe columns(LogicalType.Int32, LogicalType.Int32)
    Layout.known(PhysicalOp.MapValue(longPairs)) shouldBe
      columns(LogicalType.Int64, LogicalType.Int64)
    Layout.known(PhysicalOp.MapValue(stringPairs)) shouldBe
      columns(LogicalType.Utf8Dict, LogicalType.Int32)
  }

  it should "project one column" in {
    Layout.known(PhysicalOp.Column(stringPairs, 0)) shouldBe columns(LogicalType.Utf8Dict)
    Layout.known(PhysicalOp.Column(stringPairs, 1)) shouldBe columns(LogicalType.Int32)
    Layout.known(PhysicalOp.Column(longPairs, 0)) shouldBe columns(LogicalType.Int64)
  }

  it should "report aggregates and sorts from their kind" in {
    val summed = PhysicalOp.HashAggregate(PhysicalOp.Project(triples))
    Layout.known(summed) shouldBe columns(LogicalType.Int32, LogicalType.Int32)
    summed.schema shouldBe PhysicalSchema.IntLong
    Layout.known(PhysicalOp.HashAggregate(PhysicalOp.Exchange(pairs))) shouldBe
      columns(LogicalType.Int32, LogicalType.Int32)
    Layout.known(PhysicalOp.LongHashAggregate(PhysicalOp.Exchange(longPairs))) shouldBe
      columns(LogicalType.Int64, LogicalType.Int64)
    Layout.known(PhysicalOp.StringHashAggregate(PhysicalOp.Exchange(stringPairs))) shouldBe
      columns(LogicalType.Utf8Dict, LogicalType.Int32)
    Layout.known(PhysicalOp.CountSum(PhysicalOp.Exchange(pairs))) shouldBe
      columns(LogicalType.Int32, LogicalType.Int64, LogicalType.Int64)
    Layout.known(PhysicalOp.CountByKey(PhysicalOp.Exchange(pairs))) shouldBe
      columns(LogicalType.Int32, LogicalType.Int32)
    Layout.known(PhysicalOp.CountByKey(PhysicalOp.Exchange(longPairs))) shouldBe
      columns(LogicalType.Int64, LogicalType.Int32)
    Layout.known(PhysicalOp.CountByKey(PhysicalOp.Exchange(stringPairs))) shouldBe
      columns(LogicalType.Utf8Dict, LogicalType.Int32)
    Layout.known(PhysicalOp.CountByKey(PhysicalOp.Project(triples))) shouldBe
      columns(LogicalType.Int32, LogicalType.Int32)
    val join = PhysicalOp.HashJoin(PhysicalOp.Exchange(pairs), PhysicalOp.Exchange(pairs))
    Layout.known(PhysicalOp.CountByKey(join)) shouldBe columns(
      LogicalType.Int32,
      LogicalType.Int32,
    )
    val probe = PhysicalOp.JoinProbeSum(pairs, pairs, PhysicalOp.ProbeMeasure.Left)
    Layout.known(probe) shouldBe Layout.ProbeSum
    Layout.known(PhysicalOp.JoinProbeSum(pairs, pairs, PhysicalOp.ProbeMeasure.Right)) shouldBe
      Layout.ProbeSum
    Layout.ProbeSum should not be Layout.AssumedIntLong
    probe.schema shouldBe PhysicalSchema.IntLong
    Layout.known(PhysicalOp.SortByIntKey(pairs)) shouldBe
      columns(LogicalType.Int32, LogicalType.Int32)
    Layout.known(PhysicalOp.SortByIntValueDesc(pairs)) shouldBe
      columns(LogicalType.Int32, LogicalType.Int32)
    Layout.known(PhysicalOp.SortByLongKey(longPairs)) shouldBe
      columns(LogicalType.Int64, LogicalType.Int64)
    Layout.known(PhysicalOp.SortByStringKey(stringPairs)) shouldBe
      columns(LogicalType.Utf8Dict, LogicalType.Int32)
    val grouped = PhysicalOp.GroupByKey(PhysicalOp.Exchange(pairs))
    Layout.known(grouped) shouldBe Layout.Grouped
    Layout.Grouped shouldBe Output.Opaque("grouped values")
    Layout.known(grouped) should not be Layout.known(pairs)
    val longGrouped = PhysicalOp.GroupByKey(PhysicalOp.Exchange(longPairs))
    Layout.known(longGrouped) shouldBe Layout.Grouped
    Layout.known(longGrouped) should not be Layout.known(longPairs)
    val stringGrouped = PhysicalOp.GroupByKey(PhysicalOp.Exchange(stringPairs))
    Layout.known(stringGrouped) shouldBe Layout.Grouped
    Layout.known(stringGrouped) should not be Layout.known(stringPairs)
  }

  it should "treat grouped decode as opaque" in {
    val grouped = PhysicalOp.GroupByKey(PhysicalOp.Exchange(pairs))
    Layout.known(PhysicalOp.GroupedDecode(grouped)) shouldBe Layout.Grouped
  }

  it should "mark the missing side of an outer join absent" in {
    val exchanged = PhysicalOp.Exchange(pairs)
    val longExchanged = PhysicalOp.Exchange(longPairs)
    val stringExchanged = PhysicalOp.Exchange(stringPairs)
    val build = PhysicalOp.BuildSide.Right

    Layout.known(PhysicalOp.HashJoin(exchanged, exchanged)) shouldBe intInner
    Layout.known(PhysicalOp.SortMergeJoin(exchanged, exchanged)) shouldBe intInner
    Layout.known(PhysicalOp.BroadcastJoin(pairs, pairs, build)) shouldBe intInner
    Layout.known(PhysicalOp.HashLeftOuter(exchanged, exchanged)) shouldBe intLeft
    Layout.known(PhysicalOp.SortMergeLeftOuter(exchanged, exchanged)) shouldBe intLeft
    Layout.known(PhysicalOp.BroadcastLeftOuter(pairs, pairs)) shouldBe intLeft
    Layout.known(PhysicalOp.HashRightOuter(exchanged, exchanged)) shouldBe intRight
    Layout.known(PhysicalOp.SortMergeRightOuter(exchanged, exchanged)) shouldBe intRight
    Layout.known(PhysicalOp.BroadcastRightOuter(pairs, pairs)) shouldBe intRight
    Layout.known(PhysicalOp.HashFullOuter(exchanged, exchanged)) shouldBe intFull
    Layout.known(PhysicalOp.SortMergeFullOuter(exchanged, exchanged)) shouldBe intFull
    Layout.known(PhysicalOp.BroadcastFullOuter(pairs, pairs)) shouldBe intFull

    Layout.known(PhysicalOp.LongHashJoin(longExchanged, longExchanged)) shouldBe longInner
    Layout.known(PhysicalOp.LongBroadcastJoin(longPairs, longPairs, build)) shouldBe longInner
    Layout.known(PhysicalOp.LongSortMergeJoin(longExchanged, longExchanged)) shouldBe longInner
    Layout.known(PhysicalOp.LongHashLeftOuter(longExchanged, longExchanged)) shouldBe longLeft
    Layout.known(PhysicalOp.LongBroadcastLeftOuter(longPairs, longPairs)) shouldBe longLeft
    Layout.known(PhysicalOp.LongSortMergeLeftOuter(longExchanged, longExchanged)) shouldBe longLeft
    Layout.known(PhysicalOp.LongHashRightOuter(longExchanged, longExchanged)) shouldBe longRight
    Layout.known(PhysicalOp.LongBroadcastRightOuter(longPairs, longPairs)) shouldBe longRight
    Layout.known(PhysicalOp.LongSortMergeRightOuter(longExchanged, longExchanged)) shouldBe
      longRight
    Layout.known(PhysicalOp.LongHashFullOuter(longExchanged, longExchanged)) shouldBe longFull
    Layout.known(PhysicalOp.LongBroadcastFullOuter(longPairs, longPairs)) shouldBe longFull
    Layout.known(PhysicalOp.LongSortMergeFullOuter(longExchanged, longExchanged)) shouldBe longFull

    Layout.known(PhysicalOp.StringHashJoin(stringExchanged, stringExchanged)) shouldBe stringInner
    Layout.known(PhysicalOp.StringBroadcastJoin(stringPairs, stringPairs, build)) shouldBe
      stringInner
    Layout.known(PhysicalOp.StringSortMergeJoin(stringExchanged, stringExchanged)) shouldBe
      stringInner
    Layout.known(PhysicalOp.StringHashLeftOuter(stringExchanged, stringExchanged)) shouldBe
      stringLeft
    Layout.known(PhysicalOp.StringBroadcastLeftOuter(stringPairs, stringPairs)) shouldBe stringLeft
    Layout.known(PhysicalOp.StringSortMergeLeftOuter(stringExchanged, stringExchanged)) shouldBe
      stringLeft
    Layout.known(PhysicalOp.StringHashRightOuter(stringExchanged, stringExchanged)) shouldBe
      stringRight
    Layout.known(PhysicalOp.StringBroadcastRightOuter(stringPairs, stringPairs)) shouldBe
      stringRight
    Layout.known(PhysicalOp.StringSortMergeRightOuter(stringExchanged, stringExchanged)) shouldBe
      stringRight
    Layout.known(PhysicalOp.StringHashFullOuter(stringExchanged, stringExchanged)) shouldBe
      stringFull
    Layout.known(PhysicalOp.StringBroadcastFullOuter(stringPairs, stringPairs)) shouldBe stringFull
    Layout.known(PhysicalOp.StringSortMergeFullOuter(stringExchanged, stringExchanged)) shouldBe
      stringFull
  }

  private def columns(types: LogicalType*): Output =
    Output.Columns(types.toVector.map(column))

  private def column(tpe: LogicalType): ColumnLayout =
    val typed = tpe match
      case LogicalType.Int32 => ColumnType.Int32
      case LogicalType.Int64 => ColumnType.Int64
      case LogicalType.Float64 => ColumnType.Float64
      case LogicalType.Bool => ColumnType.Bool
      case LogicalType.Utf8Dict => ColumnType.Utf8Dict
    ColumnLayout(typed, NullPolicy.Nullable)

  private def absent(column: ColumnLayout): ColumnLayout =
    column.copy(nullPolicy = NullPolicy.Absence)

  private enum Side:
    case Inner, Left, Right, Full

  private def join(
      key: LogicalType,
      left: LogicalType,
      right: LogicalType,
      side: Side,
  ): Output =
    val keyCol = column(key)
    val leftCol = column(left)
    val rightCol = column(right)
    val marked = side match
      case Side.Inner => Vector(keyCol, leftCol, rightCol)
      case Side.Left => Vector(keyCol, leftCol, absent(rightCol))
      case Side.Right => Vector(keyCol, absent(leftCol), rightCol)
      case Side.Full => Vector(keyCol, absent(leftCol), absent(rightCol))
    Output.Columns(marked)

  private val intInner: Output =
    join(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32, Side.Inner)
  private val intLeft: Output =
    join(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32, Side.Left)
  private val intRight: Output =
    join(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32, Side.Right)
  private val intFull: Output =
    join(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32, Side.Full)
  private val longInner: Output =
    join(LogicalType.Int64, LogicalType.Int64, LogicalType.Int64, Side.Inner)
  private val longLeft: Output =
    join(LogicalType.Int64, LogicalType.Int64, LogicalType.Int64, Side.Left)
  private val longRight: Output =
    join(LogicalType.Int64, LogicalType.Int64, LogicalType.Int64, Side.Right)
  private val longFull: Output =
    join(LogicalType.Int64, LogicalType.Int64, LogicalType.Int64, Side.Full)
  private val stringInner: Output =
    join(LogicalType.Utf8Dict, LogicalType.Int32, LogicalType.Int32, Side.Inner)
  private val stringLeft: Output =
    join(LogicalType.Utf8Dict, LogicalType.Int32, LogicalType.Int32, Side.Left)
  private val stringRight: Output =
    join(LogicalType.Utf8Dict, LogicalType.Int32, LogicalType.Int32, Side.Right)
  private val stringFull: Output =
    join(LogicalType.Utf8Dict, LogicalType.Int32, LogicalType.Int32, Side.Full)
