package com.ewoodbury.scarlet.execution

/**
 * Turns one narrow [[Operation]] into a [[Stage]].
 *
 * This is the erasure boundary for stage materialization. Element types are erased when operations
 * are accumulated, and the cast returns the closure type the executor runs.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
  ),
)
private[execution] object StageFromOp:

  def apply(op: Operation[Any, Any]): Stage[Any, Any] = op match
    case MapOp(f) => Stage.map(f)
    case FilterOp(p) => Stage.filter(p)
    case FlatMapOp(f) => Stage.flatMap(f)
    case DistinctOp() => Stage.distinct[Any]
    case MapPartitionsOp(f) => Stage.mapPartitions(f)
    case PartitionByLocalOp(_) => Stage.identity[Any]
    case _: RepartitionOp[_] => Stage.identity[Any]
    case _: CoalesceOp[_] => Stage.identity[Any]
    case _: KeysOp[_, _] =>
      Stage.keys[Any, Any].asInstanceOf[Stage[Any, Any]]
    case _: ValuesOp[_, _] =>
      Stage.values[Any, Any].asInstanceOf[Stage[Any, Any]]
    case MapValuesOp(f) =>
      Stage
        .mapValues[Any, Any, Any](f.asInstanceOf[Any => Any])
        .asInstanceOf[Stage[Any, Any]]
    case MapIntKeyOp(f) =>
      Stage.mapIntKey(f).asInstanceOf[Stage[Any, Any]]
    case FilterKeysOp(p) =>
      Stage
        .filterKeys[Any, Any](p.asInstanceOf[Any => Boolean])
        .asInstanceOf[Stage[Any, Any]]
    case FilterValuesOp(p) =>
      Stage
        .filterValues[Any, Any](p.asInstanceOf[Any => Boolean])
        .asInstanceOf[Stage[Any, Any]]
    case FlatMapValuesOp(f) =>
      Stage
        .flatMapValues[Any, Any, Any](f.asInstanceOf[Any => IterableOnce[Any]])
        .asInstanceOf[Stage[Any, Any]]
    case _: GroupByKeyLocalOp[_, _] =>
      Stage.groupByKeyLocal[Any, Any].asInstanceOf[Stage[Any, Any]]
    case _: CountByKeyLocalOp[_, _] =>
      Stage.countByKeyLocal[Any, Any].asInstanceOf[Stage[Any, Any]]
    case _: CountSumByKeyLocalOp =>
      Stage.countSumByKeyLocal.asInstanceOf[Stage[Any, Any]]
    case _: CountSumByLongKeyLocalOp =>
      Stage.countSumByLongKeyLocal.asInstanceOf[Stage[Any, Any]]
    case _: CountSumByStringKeyLocalOp =>
      Stage.countSumByStringKeyLocal.asInstanceOf[Stage[Any, Any]]
    case _: CountSumByIntLongKeyLocalOp =>
      Stage.countSumByIntLongKeyLocal.asInstanceOf[Stage[Any, Any]]
    case ExtremumByKeyLocalOp(minimum) =>
      Stage.extremumByKeyLocal(minimum).asInstanceOf[Stage[Any, Any]]
    case ReduceByKeyLocalOp(reduceFunc) =>
      Stage
        .reduceByKeyLocal[Any, Any](reduceFunc.asInstanceOf[(Any, Any) => Any])
        .asInstanceOf[Stage[Any, Any]]
    case WideFilterOp(ordinal, compare) =>
      Stage.wideFilter(ordinal, compare).asInstanceOf[Stage[Any, Any]]
    case WideProjectOp(spec) =>
      Stage.wideProject(spec).asInstanceOf[Stage[Any, Any]]
    case WideComputeOp(expr, name) =>
      Stage.wideCompute(expr, name).asInstanceOf[Stage[Any, Any]]
    case _ =>
      throw new UnsupportedOperationException(s"Cannot create stage from operation: $op")
