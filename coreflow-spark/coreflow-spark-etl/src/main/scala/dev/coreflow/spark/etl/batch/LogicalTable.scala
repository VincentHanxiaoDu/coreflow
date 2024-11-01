package dev.coreflow.spark.etl.batch

import scala.util.matching.Regex

/**
 * <LogicalTable> is a trait that represents a logical Spark table.
 */
trait LogicalTable {
  private val tableNameRegex: Regex = "^[a-zA-Z_][a-zA-Z0-9_]*$".r

  def tableName: String

  require(Option(tableName).isDefined, "tableName cannot be null")

  require(tableName.nonEmpty, "tableName cannot be empty")

  require(
    tableNameRegex.pattern.matcher(tableName).matches(),
    s"Invalid table name: '$tableName'. A valid table name must start with a letter or " +
      s"underscore, " +
      "and contain only letters, digits, or underscores."
  )
}