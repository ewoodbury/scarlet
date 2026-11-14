package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan

/**
 * Contract fixtures for one map directly above int, string, and long `groupByKey`.
 *
 * Each plan lowers to `GroupedDecode(GroupByKey(Exchange(Scan)))`. The layout stays the grouped
 * layout of that group. These three plans are new accepted fixtures. They are not exclusions.
 */
private[execution] object GroupedDecodeCases:

  val shape: String = "GroupedDecode(GroupByKey(Exchange(Scan)))"

  enum Family:
    case IntGroup, StringGroup, LongGroup

  final case class Fixture(label: String, plan: Plan[_], family: Family)

  def fixtures(
      pairs: Plan[(Int, Int)],
      strings: Plan[(String, Int)],
      longs: Plan[(Long, Long)],
  ): Seq[Fixture] = Seq(
    Fixture(
      "map above int groupByKey",
      Plan.MapOp(Plan.GroupByKeyOp(pairs), (row: (Int, Iterable[Int])) => row._1),
      Family.IntGroup,
    ),
    Fixture(
      "map above string groupByKey",
      Plan.MapOp(Plan.GroupByKeyOp(strings), (row: (String, Iterable[Int])) => row._1),
      Family.StringGroup,
    ),
    Fixture(
      "map above long groupByKey",
      Plan.MapOp(Plan.GroupByKeyOp(longs), (row: (Long, Iterable[Long])) => row._1),
      Family.LongGroup,
    ),
  )

  def layout(family: Family, batchSize: Int): Layout = family match
    case Family.IntGroup => Layout.GroupedInt(batchSize)
    case Family.StringGroup => Layout.GroupedString(batchSize)
    case Family.LongGroup => Layout.GroupedLong(batchSize)
