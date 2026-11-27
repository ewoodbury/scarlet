package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * TPC-H queries 6, 1, and 3 on generated rows. This is not a downloaded scale factor.
 *
 * Money is a long. A date is a long. An average is a sum divided by a count. Query 1 groups by the
 * return flag only. Query 3 multiplies the lineitem price by the discount, joins orders, and
 * groups by the order key. [[note]] names every TPC-H query.
 */
object TpchSlice:

  val lineitemScale: String = "rows-4096"
  val ordersScale: String = "orders-256"
  val lineitemRows: Int = 4096
  val orderRows: Int = 256
  val partitions: Int = 2

  /** Rows whose ship, discount, and quantity pass. Price times discount. */
  val revenue: Long = 1035571L
  val revenueRows: Long = 344L

  /** Return flags 0, 1, and 2. Count, sum, min, and max of quantity. Ordered by the flag. */
  val pricingMix: Long = -2219730004622530266L
  val pricingGroups: Long = 3L

  /** Order keys 0 to 127. Each count is 1 and each revenue is 30. Revenue descending, then key. */
  val shippingMix: Long = 414472905078466624L
  val shippingGroups: Long = 128L

  val note: Vector[String] = Vector(
    "1 This file. One group column. The published query also groups by line status.",
    "2 Missing: subquery.",
    "3 This file. One join and one group column. The published query also joins customer.",
    "4 Missing: exists.",
    "5 Missing: several joins.",
    "6 This file.",
    "7 Missing: several joins.",
    "8 Missing: several joins.",
    "9 Missing: several joins.",
    "10 Missing: several joins.",
    "11 Missing: subquery.",
    "12 Missing: several joins.",
    "13 Missing: outer join.",
    "14 Missing: string LIKE.",
    "15 Missing: subquery.",
    "16 Missing: subquery.",
    "17 Missing: subquery.",
    "18 Missing: several joins.",
    "19 Missing: several joins.",
    "20 Missing: subquery.",
    "21 Missing: exists.",
    "22 Missing: subquery.",
  )

  def main(args: Array[String]): Unit =
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(2)
    try
      ScarletConf.set(
        savedConf.copy(
          threadPoolSize = 2,
          columnarBatchSize = 8192,
          defaultShufflePartitions = partitions,
          columnarExecution = true,
          spillEnabled = false,
        ),
      )
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      val problems = failures()
      if problems.nonEmpty then
        System.err.println(s"tpch slice failed: ${problems.mkString("; ")}")
        sys.exit(1)
    finally
      try scheduler.shutdown()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  /** Columnar lowering, then the same checksum on the row path. Empty means the slice passed. */
  def failures(): List[String] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = true, spillEnabled = false))
      prove() ++ bothPaths()
    finally ScarletConf.set(saved)

  def revenuePlan(): DistCollection[WideRow] =
    lineitem()
      .filterColumn(0, ColumnCompare(CompareOp.Ge, ColumnLiteral.Int64(100L)))
      .filterColumn(0, ColumnCompare(CompareOp.Lt, ColumnLiteral.Int64(200L)))
      .filterColumn(1, ColumnCompare(CompareOp.Ge, ColumnLiteral.Int64(5L)))
      .filterColumn(1, ColumnCompare(CompareOp.Le, ColumnLiteral.Int64(7L)))
      .filterColumn(2, ColumnCompare(CompareOp.Lt, ColumnLiteral.Int64(24L)))
      .compute(ColumnExpr.Mul(3, 1), "revenue")

  def pricingPlan(): DistCollection[WideRow] =
    pricing()
      .filterColumn(0, ColumnCompare(CompareOp.Le, ColumnLiteral.Int64(3000L)))
      .groupColumns(1, 2)

  def shippingPlan(): DistCollection[WideRow] =
    orders()
      .hashJoinColumns(lines().compute(ColumnExpr.Mul(2, 3), "revenue"), 0, 0)
      .groupColumns(
        0,
        5,
      )

  def pricingChecksum(rows: Iterable[WideRow]): Long =
    rows.toVector.sortWith(byKey).foldLeft(0L)(mixIntMeasure)

  def shippingChecksum(rows: Iterable[WideRow]): Long =
    rows.toVector.sortWith(byRevenue).foldLeft(0L)(mixLongMeasure)

  private def prove(): List[String] =
    val revenueShape =
      "WideCompute(WideFilter2(WideFilter1(WideFilter1(WideFilter0(WideFilter0(Scan))))))"
    List(
      shaped("query 6", revenuePlan().plan, revenueShape),
      summed("query 6", revenuePlan().plan, 4, revenue),
      counted("query 6", revenuePlan().plan, revenueRows),
      shaped("query 1", pricingPlan().plan, "WideGroup(WideFilter0(Scan))"),
      counted("query 1", pricingPlan().plan, pricingGroups),
      shaped(
        "query 3",
        shippingPlan().plan,
        "WideGroup(WideHashJoin(WideFilter1(Scan),WideCompute(WideFilter1(Scan))))",
      ),
      counted("query 3", shippingPlan().plan, shippingGroups),
    ).flatten

  private def bothPaths(): List[String] =
    List(
      both("query 6", revenue)(revenuePlan().sumLongColumn(4)),
      both("query 1", pricingMix)(pricingChecksum(pricingPlan().collect())),
      both("query 3", shippingMix)(shippingChecksum(shippingPlan().collect())),
    ).flatten

  private def lineitem(): DistCollection[WideRow] =
    DistCollection.ofColumns(
      Vector(
        WideInput.longs("ship", Array.tabulate(lineitemRows)(index => 50L + (index % 200).toLong)),
        WideInput.longs("discount", Array.tabulate(lineitemRows)(index => (index % 11).toLong)),
        WideInput.longs("quantity", Array.tabulate(lineitemRows)(index => (index % 40).toLong)),
        WideInput.longs(
          "price",
          Array.tabulate(lineitemRows)(index => ((index * 17) % 1000).toLong),
        ),
      ),
      partitions,
    )

  private def pricing(): DistCollection[WideRow] =
    DistCollection.ofColumns(
      Vector(
        WideInput.longs("ship", Array.tabulate(lineitemRows)(index => index.toLong)),
        WideInput.ints("flag", Array.tabulate(lineitemRows)(index => index % 3)),
        WideInput.ints("qty", Array.tabulate(lineitemRows)(index => 1 + (index % 50))),
      ),
      partitions,
    )

  private def orders(): DistCollection[WideRow] =
    DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("orderkey", Array.tabulate(orderRows)(index => index)),
          WideInput.longs("orderdate", Array.tabulate(orderRows)(index => index.toLong)),
        ),
        partitions,
      )
      .filterColumn(1, ColumnCompare(CompareOp.Lt, ColumnLiteral.Int64(128L)))

  private def lines(): DistCollection[WideRow] =
    DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("orderkey", Array.tabulate(orderRows)(index => index)),
          WideInput.longs(
            "shipdate",
            Array.tabulate(orderRows)(index => if index < 128 then 40L else 10L),
          ),
          WideInput.longs("price", Array.tabulate(orderRows)(_ => 10L)),
          WideInput.longs("discount", Array.tabulate(orderRows)(_ => 3L)),
        ),
        partitions,
      )
      .filterColumn(1, ColumnCompare(CompareOp.Gt, ColumnLiteral.Int64(32L)))

  private def both(name: String, expected: Long)(query: => Long): Option[String] =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    val columnar = query
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = query
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    if columnar != expected then Some(s"$name columnar $columnar expected $expected")
    else if rows != expected then Some(s"$name rows $rows expected $expected")
    else None

  private def counted(name: String, plan: Plan[WideRow], expected: Long): Option[String] =
    ColumnarPlanner.tryCount(plan) match
      case Some(value) if value == expected => None
      case Some(value) => Some(s"$name columnar count $value expected $expected")
      case None => Some(s"$name did not lower")

  private def summed(
      name: String,
      plan: Plan[WideRow],
      ordinal: Int,
      expected: Long,
  ): Option[String] =
    WideSum.longColumn(plan, ordinal) match
      case Some(value) if value == expected => None
      case Some(value) => Some(s"$name columnar sum $value expected $expected")
      case None => Some(s"$name sum did not lower")

  private def shaped(name: String, plan: Plan[WideRow], shape: String): Option[String] =
    ColumnarPlanner.lower(plan).map(_.shape) match
      case Some(found) if found == shape => None
      case Some(found) => Some(s"$name shape $found expected $shape")
      case None => Some(s"$name did not lower")

  private def byKey(left: WideRow, right: WideRow): Boolean = keyOf(left) < keyOf(right)

  private def byRevenue(left: WideRow, right: WideRow): Boolean =
    val leftSum = sumOf(left)
    val rightSum = sumOf(right)
    leftSum > rightSum || (leftSum == rightSum && keyOf(left) < keyOf(right))

  private def mixIntMeasure(acc: Long, row: WideRow): Long = cells(row) match
    case Vector(
          Cell.Int32(key),
          Cell.Int64(count),
          Cell.Int64(sum),
          Cell.Int32(min),
          Cell.Int32(max),
        ) =>
      ((((acc * 31L + key.toLong) * 31L + count) * 31L + sum) * 31L + min.toLong) * 31L +
        max.toLong
    case other =>
      refuse(other.size)

  private def mixLongMeasure(acc: Long, row: WideRow): Long = cells(row) match
    case Vector(
          Cell.Int32(key),
          Cell.Int64(count),
          Cell.Int64(sum),
          Cell.Int64(min),
          Cell.Int64(max),
        ) =>
      ((((acc * 31L + key.toLong) * 31L + count) * 31L + sum) * 31L + min) * 31L + max
    case other =>
      refuse(other.size)

  private def keyOf(row: WideRow): Long = cells(row) match
    case Vector(Cell.Int32(key), _, _, _, _) => key.toLong
    case Vector(Cell.Int64(key), _, _, _, _) => key
    case other => refuse(other.size)

  private def sumOf(row: WideRow): Long = cells(row) match
    case Vector(_, _, Cell.Int64(sum), _, _) => sum
    case other => refuse(other.size)

  private def cells(row: WideRow): Vector[Cell] = row.cells

  private def refuse(width: Int): Nothing =
    throw new IllegalArgumentException(s"group row has $width cells")
