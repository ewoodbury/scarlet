package com.ewoodbury.scarlet.sql

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Plan, WideRow}

/** A converted statement. A scalar carries the plan the aggregate read. */
enum SqlResult:
  case Value(result: Long, plan: Plan[WideRow])
  case Rows(rows: DistCollection[WideRow])

/**
 * Turns one SELECT into the wide builders.
 *
 * The parser dependency stays in this module. A statement outside the closed set throws, and the
 * message names the node.
 */
object Sql:

  def run(statement: String, tables: Map[String, DistCollection[WideRow]]): SqlResult =
    require(statement.nonEmpty, "sql must not be empty")
    SqlConvert.run(statement, tables)
