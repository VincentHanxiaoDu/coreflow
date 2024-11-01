package dev.coreflow.spark.etl.batch

import scala.util.matching.Regex

/**
 * Materialized Spark table.
 */
trait MaterializedTable extends LogicalTable {
  def tableDB: String

  private val tableDBRegex: Regex = "^[a-zA-Z_][a-zA-Z0-9_]*$".r

  require(Option(tableDB).isDefined, "tableDB cannot be null")

  require(tableDB.nonEmpty, "tableDB cannot be empty")

  require(
    tableDBRegex.pattern.matcher(tableDB).matches(),
    s"Invalid database name: '$tableDB'. A valid database name must start with a letter or " +
      s"underscore, " +
      "and contain only letters, digits, or underscores."
  )
}
