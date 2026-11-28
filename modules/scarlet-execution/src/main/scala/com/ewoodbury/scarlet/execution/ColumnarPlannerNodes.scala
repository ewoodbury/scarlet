package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

import com.ewoodbury.scarlet.api.DictionaryStringPredicate
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}

import ColumnarPlanner.ElementStep

/**
 * Node cases for [[ColumnarPlanner]].
 *
 * `lowerPlan` stays on the planner. These cases call it for a child, and it calls them for a node.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Throw",
    "org.wartremover.warts.Null",
  ),
)
private[execution] trait ColumnarPlannerNodes:
  protected def lowerPlan(plan: Plan[_]): Option[PhysicalOp]

  /**
   * Lower `plan` when a parent can wrap it. [[SortLowering.acceptsAsChild]] refuses a sort, so
   * this does not wrap one. [[filterOverKeySort]] is the exception for one filter over an int,
   * long, or string key sort, and only when that key column is fully present. A join, aggregate,
   * or projection above a sort stays on the row path.
   */
  protected def lowerChild(plan: Plan[_]): Option[PhysicalOp] =
    lowerPlan(plan).filter(SortLowering.acceptsAsChild)

  protected def lowerPlanNode(plan: Plan[_]): Option[PhysicalOp] = plan match
    case Plan.UnionOp(left, right) =>
      // A key map can still hold a null key. Union would publish it, and the pair decoder
      // rejects a clear bit. That shape stays on the row path.
      UnionLowering.lower(left, right, lowerChild).filterNot(MapIntKey.inTree)
    case Plan.DistinctOp(source) =>
      lowerChild(source).flatMap(DistinctLowering.apply)
    case source: Plan.Source[_] =>
      ScanLowering.classify(source).map(kind => PhysicalOp.Scan(kind, source.partitions))
    case Plan.FilterKeysOp(source, _) =>
      columnFilter(source, PhysicalOp.Slot.Key)
    case Plan.FilterValuesOp(source, _) =>
      columnFilter(source, PhysicalOp.Slot.Value)
    case Plan.KeysOp(Plan.GroupByKeyOp(source)) =>
      KeyValueLowering.groupKeys(source, lowerChild)
    case Plan.KeysOp(source) =>
      KeyValueLowering.project(source, 0, lowerChild)
    case Plan.ValuesOp(source) =>
      KeyValueLowering.project(source, 1, lowerChild)
    case Plan.MapValuesOp(source, mapper) =>
      if (returnsUnboxedInt(mapper)) then mapValue(source, PhysicalOp.Kind.Pairs)
      else if (returnsUnboxedLong(mapper)) then mapValue(source, PhysicalOp.Kind.LongPairs)
      else if (FunctionWitness.declaredIntToLong(mapper)) then intLongMapValue(source)
      else None
    case Plan.MapIntKeyOp(source, _) =>
      lowerChild(source).flatMap { child =>
        // A second key map above a slot filter would publish the filtered key. Two maps with
        // no filter between them still lower. An int-to-long value is not an int pair.
        if (MapIntKey.filtered(child) || intLongMapped(child)) then None
        else
          child.kind match
            case PhysicalOp.Kind.Pairs => Some(PhysicalOp.MapKey(child))
            case _ => None
      }
    case Plan.ReduceByKeyOp(source, reduce) =>
      JoinProbeAggregate
        .lower(source, reduce, lowerChild)
        .orElse(AggregateBinding.reduce(source, reduce, lowerChild))
    case Plan.GroupByKeyOp(source) =>
      lowerChild(source).flatMap(GroupByKeyLowering.apply)
    case Plan.CountByKeyOp(source) =>
      AggregateBinding.count(source, lowerChild)
    case Plan.CountSumByKeyOp(source) =>
      AggregateBinding.countSum(source, lowerChild)
    case Plan.CountSumByLongKeyOp(source) =>
      AggregateBinding.countSum(source, lowerChild)
    case Plan.CountSumByStringKeyOp(source) =>
      AggregateBinding.countSum(source, lowerChild)
    case Plan.CountSumByIntLongKeyOp(source) =>
      AggregateBinding.countSum(source, lowerChild)
    case Plan.ExtremumByKeyOp(source, minimum) =>
      AggregateBinding.extremum(source, minimum, lowerChild)
    case Plan.WideFilterOp(source, ordinal, compare) =>
      WideLowering.filter(source, ordinal, compare, lowerChild)
    case Plan.WideProjectOp(source, spec) =>
      WideLowering.project(source, spec, lowerChild)
    case Plan.WideComputeOp(source, expr, name) =>
      WideLowering.compute(source, expr, name, lowerChild)
    case Plan.WideJoinOp(left, right, leftKey, rightKey, broadcast) =>
      WideLowering.join(left, right, leftKey, rightKey, broadcast, lowerChild)
    case Plan.WideLeftJoinOp(left, right, leftKey, rightKey, broadcast) =>
      WideLowering.leftJoin(left, right, leftKey, rightKey, broadcast, lowerChild)
    case Plan.WideGroupOp(source, key, measure, key2, distinct, textMin, sum2) =>
      WideLowering.group(source, key, measure, key2, distinct, textMin, sum2, lowerChild)
    case Plan.WideSortOp(source, ordinal, descending, second) =>
      WideLowering.sort(source, ordinal, descending, second, lowerChild)
    case Plan.WideLimitOp(source, count) =>
      WideLowering.limit(source, count, lowerChild)
    case plan if JoinLowering.isJoin(plan) => JoinLowering.lower(plan, lowerChild)
    case Plan.SortByIntKeyOp(source) =>
      SortLowering.lowerInt(source, lowerPlan)
    case Plan.SortByIntValueDescOp(source) =>
      SortLowering.lowerIntValueDesc(source, lowerPlan)
    case Plan.SortByLongKeyOp(source) =>
      SortLowering.lowerLong(source, lowerPlan)
    case Plan.SortByStringKeyOp(source) =>
      SortLowering.lowerString(source, lowerPlan)
    case Plan.SortByLongValueDescOp(source) =>
      SortLowering.lowerLongValue(source, lowerPlan)
    case Plan.SortByStringValueDescOp(source) =>
      SortLowering.lowerStringValue(source, lowerPlan)
    case Plan.SortByIntLongKeyOp(source) =>
      SortLowering.lowerIntLongKey(source, lowerPlan)
    case Plan.SortByIntLongValueDescOp(source) =>
      SortLowering.lowerIntLongValue(source, lowerPlan)
    case _ => None

  private[execution] def elementSteps(
      plan: Plan[_],
      limit: Int,
  ): (Plan[_], List[ElementStep]) =
    @tailrec
    def collect(
        current: Plan[_],
        remaining: Int,
        steps: List[ElementStep],
    ): (Plan[_], List[ElementStep]) =
      if (remaining == 0) then (current, steps)
      else
        current match
          case Plan.FilterOp(source, predicate) =>
            collect(source, remaining - 1, ElementStep.Filter(predicate) :: steps)
          case Plan.MapOp(source, mapper) =>
            collect(source, remaining - 1, ElementStep.Project(mapper) :: steps)
          case _ => (current, steps)

    collect(plan, limit, Nil)

  protected def lowerElementSteps(
      child: PhysicalOp,
      steps: List[ElementStep],
  ): Option[PhysicalOp] = child.kind match
    case PhysicalOp.Kind.Bools =>
      if (FilterBinding.element(child, steps)) then BoolPipeline.lower(child, steps) else None
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Strings =>
      // The kind names this element chain. [[ProjectBinding]] asks for [[KernelChoice.Project]]
      // or [[KernelChoice.Pipeline]]. This match does not pick the class.
      ProjectBinding.chain(child, steps)
    case PhysicalOp.Kind.Float32s =>
      Float32Steps.lower(child, steps)
    case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongPairs =>
      steps.foldLeft(Option(child)) { (current, step) =>
        current.flatMap(op => lowerElementStep(op, step))
      }
    case PhysicalOp.Kind.StringPairs =>
      steps.foldLeft(Option(child)) { (current, step) =>
        current.flatMap(op => lowerStringRow(op, step))
      }
    case PhysicalOp.Kind.LongCounts | PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.StringTriples |
        PhysicalOp.Kind.GroupedInts | PhysicalOp.Kind.GroupedStrings |
        PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter | PhysicalOp.Kind.StringOuter |
        PhysicalOp.Kind.CountSums | PhysicalOp.Kind.LongCountSums |
        PhysicalOp.Kind.StringCountSums | PhysicalOp.Kind.IntLongSums |
        PhysicalOp.Kind.GroupedIntLongs | PhysicalOp.Kind.IntLongTriples |
        PhysicalOp.Kind.IntLongOuter | PhysicalOp.Kind.Columns =>
      None

  /**
   * A whole-row string filter, or one `(String, Int) => (String, Int)` project. A whole-row filter
   * above a string-pair project stays on the row path. A project above a slot filter on that
   * project stays there too. [[SortLowering.lowerString]] admits one such project under the sort.
   */
  protected def lowerStringRow(child: PhysicalOp, step: ElementStep): Option[PhysicalOp] =
    step match
      case ElementStep.Filter(_) if StringPairSlot.containsProject(child) => None
      case ElementStep.Filter(_) => FilterBinding.fixed(child, PhysicalOp.Slot.Row)
      case ElementStep.Project(mapper)
          if returnsIntPair(mapper) && !StringPairSlot.filterCoversProject(child) =>
        Some(PhysicalOp.Project(child))
      case ElementStep.Project(_) => None

  protected def lowerElementStep(child: PhysicalOp, step: ElementStep): Option[PhysicalOp] =
    // A whole-row filter or a project above a key map sees a null key the row path unboxes.
    if (MapIntKey.inTree(child)) then None
    else
      step match
        // A whole-row filter above a long-pair project stays on the row path. A filter over a
        // scan, or over a long-pair filter stack that has no project, still lowers.
        case ElementStep.Filter(_) if LongPairSlot.containsProject(child) => None
        case ElementStep.Filter(predicate) =>
          FilterBinding.row(child, predicate)
        case ElementStep.Project(_) if intLongMapped(child) => None
        case ElementStep.Project(mapper) =>
          child.kind match
            case PhysicalOp.Kind.Triples if child.isInnerTriple =>
              Some(PhysicalOp.Project(child))
            case PhysicalOp.Kind.Pairs if returnsIntPair(mapper) =>
              Some(PhysicalOp.Project(child))
            case PhysicalOp.Kind.LongPairs if returnsIntPair(mapper) =>
              Some(PhysicalOp.Project(child))
            case _ => None

  protected val MaxDictionaryStringLiterals = 8

  private[execution] def dictionaryStringLiterals(predicate: Any): Option[Vector[String]] =
    predicate match
      case recognized: DictionaryStringPredicate[?] =>
        val literals = recognized.literals
        Option.when(
          literals.nonEmpty &&
            literals.length <= MaxDictionaryStringLiterals &&
            literals.forall(presentLiteral),
        )(literals)
      case _ => None

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  protected def presentLiteral(value: String): Boolean = value match
    case null => false
    case _ => true

  /**
   * Int value to long value. The kind check runs before the ask, so a string pair and a long pair
   * leave [[ColumnBinding.chooseCallsForTest]] at zero. [[MapIntKey.inTree]] refuses first.
   */
  protected def intLongMapValue(source: Plan[_]): Option[PhysicalOp] =
    lowerChild(source).flatMap { child =>
      if (MapIntKey.inTree(child)) then None
      else
        child.kind match
          case PhysicalOp.Kind.Pairs => IntLongMapBinding.value(child)
          case _ => None
    }

  /**
   * The widened map, or a filter or long `mapValues` on it. A parent that matches
   * [[PhysicalOp.Kind.Pairs]] must not build an int-pair project or a key map on it.
   */
  protected def intLongMapped(op: PhysicalOp): Boolean =
    IntLongParents.widened(op)

  protected def mapValue(source: Plan[_], kind: PhysicalOp.Kind): Option[PhysicalOp] =
    lowerChild(source).flatMap { child =>
      // mapValues keeps a null key. The pair decoder of that parent rejects the clear bit.
      if (MapIntKey.inTree(child)) then None
      else
        // The kind agrees the value column with the witness. [[MapValueBinding]] asks.
        // This match does not build the class.
        (child.kind, kind) match
          case (PhysicalOp.Kind.Pairs | PhysicalOp.Kind.StringPairs, PhysicalOp.Kind.Pairs) =>
            MapValueBinding.value(child)
          case (PhysicalOp.Kind.LongPairs, PhysicalOp.Kind.LongPairs) =>
            MapValueBinding.value(child)
          // [[ColumnBinding.asked]] stays [[scala.None]] for this layout, so the long map is not
          // a binder tag. The value column is already `Int64`.
          case (_, PhysicalOp.Kind.LongPairs) if IntLongParents.widened(child) =>
            Some(PhysicalOp.MapValue(child))
          case _ => None
    }

  /** Unboxed `Int => Int` via [[FunctionWitness]], same as an int element project. */
  protected def returnsUnboxedInt(function: Any): Boolean =
    FunctionWitness.declaredElement(PhysicalOp.Kind.Ints, function)

  /**
   * `Tuple2 => Tuple2` via [[FunctionWitness]], the same witness shape as [[returnsUnboxedInt]].
   * Erasure does not separate `(Int, Int)` from `(Long, Long)` or `(Long, Int)`. The child kind
   * selects the cell. The int-pair kernel declines a cell that is not an int. The long-pair kernel
   * declines a cell that is not a long. The string-pair kernel declines a cell that is not a
   * string or an int. This does not call `function` and does not read its body.
   * [[FunctionWitness.declaredElement]] stays false for [[PhysicalOp.Kind.LongPairs]] and
   * [[PhysicalOp.Kind.StringPairs]].
   */
  protected def returnsIntPair(function: Any): Boolean =
    FunctionWitness.declaredElement(PhysicalOp.Kind.Pairs, function)

  /** Unboxed `Long => Long` via [[FunctionWitness]], same as a long element project. */
  protected def returnsUnboxedLong(function: Any): Boolean =
    FunctionWitness.declaredElement(PhysicalOp.Kind.Longs, function)

  protected def columnFilter(source: Plan[_], slot: PhysicalOp.Slot): Option[PhysicalOp] =
    lowerChild(source).flatMap { child =>
      // One key or value slot above one int key map, one string-pair project, or one long-pair
      // project. The predicate is not read. Every other child asks [[FilterBinding]]. One slot
      // above an int-long project stays there. A second slot, and an unprojected join, stay empty.
      if (
          MapIntKey.slotAbove(child, slot) || StringPairSlot.oneProject(child) ||
          LongPairSlot.oneProject(child)
        )
      then Some(PhysicalOp.Filter(child, slot))
      else if (
          MapIntKey.inTree(child) || LongPairSlot.containsProject(child) ||
          StringPairSlot.containsProject(child)
        )
      then None
      else FilterBinding.slot(child, slot)
    }

  private[execution] def infoOf(op: PhysicalOp): PartitioningInfo = op match
    case PhysicalOp.Filter(child, _) => infoOf(child)
    case PhysicalOp.Project(child) => infoOf(child)
    case PhysicalOp.Pipeline(child, _) => infoOf(child)
    case union: PhysicalOp.Union =>
      UnionLowering.info(union, infoOf, layoutOf(op.kind))
    case PhysicalOp.Dedupe(_) => hashed(op.kind)
    case column @ PhysicalOp.Column(child, _) =>
      infoOf(child).withoutPairDistribution.copy(layout = layoutOf(column.kind))
    case PhysicalOp.MapValue(child) => infoOf(child)
    case PhysicalOp.IntLongMapValue(child) => infoOf(child)
    case PhysicalOp.MapKey(child) => infoOf(child).withoutPairDistribution
    case _: PhysicalOp.SortByIntKey | _: PhysicalOp.SortByLongKey =>
      SortLowering.info(shuffleWidth, layoutOf(PhysicalOp.Kind.Pairs))
    case _: PhysicalOp.SortByIntValueDesc =>
      SortLowering.infoValueDesc(shuffleWidth, layoutOf(PhysicalOp.Kind.Pairs))
    case _: PhysicalOp.SortByStringKey =>
      SortLowering.info(shuffleWidth, layoutOf(PhysicalOp.Kind.StringPairs))
    case _: PhysicalOp.SortByLongValueDesc =>
      SortLowering.infoValueDesc(shuffleWidth, layoutOf(PhysicalOp.Kind.LongPairs))
    case _: PhysicalOp.SortByStringValueDesc =>
      SortLowering.infoValueDesc(shuffleWidth, layoutOf(PhysicalOp.Kind.StringPairs))
    case _: PhysicalOp.SortByIntLongKey =>
      SortLowering.info(shuffleWidth, layoutOf(PhysicalOp.Kind.Pairs))
    case _: PhysicalOp.SortByIntLongValueDesc =>
      SortLowering.infoValueDesc(shuffleWidth, layoutOf(PhysicalOp.Kind.Pairs))
    case _: PhysicalOp.HashAggregate | _: PhysicalOp.LongHashAggregate =>
      hashed(PhysicalOp.Kind.Pairs)
    case _: PhysicalOp.StringHashAggregate =>
      hashed(PhysicalOp.Kind.StringPairs)
    case group: PhysicalOp.GroupByKey =>
      hashed(group.kind)
    case PhysicalOp.GroupedDecode(child) => infoOf(child)
    case count: PhysicalOp.CountByKey =>
      hashed(count.kind)
    case _: PhysicalOp.CountSum =>
      hashed(PhysicalOp.Kind.CountSums)
    case _: PhysicalOp.LongCountSum =>
      hashed(PhysicalOp.Kind.LongCountSums)
    case _: PhysicalOp.StringCountSum =>
      hashed(PhysicalOp.Kind.StringCountSums)
    case _: PhysicalOp.IntLongHashAggregate =>
      hashed(PhysicalOp.Kind.Pairs)
    case _: PhysicalOp.IntLongCountSum =>
      hashed(PhysicalOp.Kind.CountSums)
    case _: PhysicalOp.IntExtremum =>
      hashed(PhysicalOp.Kind.Pairs)
    case PhysicalOp.WideFilter(child, _, _) => infoOf(child)
    case PhysicalOp.WideProject(child, _) => infoOf(child)
    case PhysicalOp.WideCompute(child, _, _) => infoOf(child)
    case _: PhysicalOp.WideHashJoin | _: PhysicalOp.WideBroadcastJoin |
        _: PhysicalOp.WideHashLeftJoin | _: PhysicalOp.WideBroadcastLeftJoin |
        _: PhysicalOp.WideGroup =>
      hashed(PhysicalOp.Kind.Columns)
    case _: PhysicalOp.WideSort | _: PhysicalOp.WideLimit =>
      narrow(Distribution.Unknown(1), layoutOf(PhysicalOp.Kind.Columns))
    case _: PhysicalOp.JoinProbeSum =>
      hashed(PhysicalOp.Kind.IntLongSums)
    case PhysicalOp.Exchange(_) =>
      throw new IllegalStateException("exchange is only a child of aggregate or join")
    case PhysicalOp.Scan(kind, partitions) =>
      narrow(Distribution.Unknown(partitions.length), layoutOf(kind))
    case join =>
      JoinLowering.info(join, infoOf, layoutOf, hashed)

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private[execution] def stringResult(value: Any): String = value match
    case null => null
    case text: String => text
    case other =>
      throw new ColumnarFallback(
        s"columnar string map expected a String, found ${other.getClass.getName}",
      )

  protected def narrow(distribution: Distribution, chosen: Layout): PartitioningInfo =
    PartitioningInfo(distribution, OrderingTag.Unsorted, chosen)

  protected def hashed(kind: PhysicalOp.Kind): PartitioningInfo =
    PartitioningInfo(
      Distribution.Hash(shuffleWidth, PartitioningInfo.RowKeyOrdinals),
      OrderingTag.Unsorted,
      layoutOf(kind),
    )

  /** String plans name the dictionary layout. String pairs and string joins name their columns. */
  protected def layoutOf(kind: PhysicalOp.Kind): Layout = kind match
    case PhysicalOp.Kind.Strings => Layout.Utf8Dict(ScarletConf.get.columnarBatchSize)
    case PhysicalOp.Kind.StringPairs => Layout.StringPair(ScarletConf.get.columnarBatchSize)
    case PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.StringOuter =>
      Layout.StringTriple(ScarletConf.get.columnarBatchSize)
    case PhysicalOp.Kind.GroupedInts => Layout.GroupedInt(ScarletConf.get.columnarBatchSize)
    case PhysicalOp.Kind.GroupedStrings =>
      Layout.GroupedString(ScarletConf.get.columnarBatchSize)
    case PhysicalOp.Kind.GroupedLongs =>
      Layout.GroupedLong(ScarletConf.get.columnarBatchSize)
    case PhysicalOp.Kind.GroupedIntLongs =>
      Layout.GroupedIntLong(ScarletConf.get.columnarBatchSize)
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Bools | PhysicalOp.Kind.Pairs | PhysicalOp.Kind.Triples |
        PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.LongCounts | PhysicalOp.Kind.LongTriples |
        PhysicalOp.Kind.LongOuter | PhysicalOp.Kind.CountSums | PhysicalOp.Kind.LongCountSums |
        PhysicalOp.Kind.StringCountSums | PhysicalOp.Kind.IntLongSums |
        PhysicalOp.Kind.IntLongTriples | PhysicalOp.Kind.IntLongOuter | PhysicalOp.Kind.Float32s |
        PhysicalOp.Kind.Columns =>
      Layout.Columnar(ScarletConf.get.columnarBatchSize)

  private[execution] def batchSize: Int =
    val size = ScarletConf.get.columnarBatchSize
    require(size > 0, s"columnar batch size must be positive, got $size")
    size

  private[execution] def parallelism: Int =
    val workers = ScarletConf.get.threadPoolSize
    require(workers > 0, s"thread pool size must be positive, got $workers")
    workers

  private[execution] def shuffleWidth: Int =
    val width = ScarletConf.get.defaultShufflePartitions
    require(width > 0, s"shuffle partitions must be positive, got $width")
    width
