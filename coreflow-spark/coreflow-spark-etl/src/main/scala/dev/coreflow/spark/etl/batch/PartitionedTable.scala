package dev.coreflow.spark.etl.batch

import scala.collection.immutable.HashSet

/**
 * <PartitionedTable> is a trait that represents a partitioned Spark table.
 */
trait PartitionedTable extends LogicalTable {
  def partitionColumns: HashSet[String]

  require(
    partitionColumns.nonEmpty,
    "partitionColumns cannot be empty"
  )

  /**
   * @param columns The columns to check.
   * @return True if the columns are a subset of the partition columns.
   */
  def isPartitionSubset(columns: Set[String]): Boolean = {
    columns.subsetOf(partitionColumns)
  }
}
