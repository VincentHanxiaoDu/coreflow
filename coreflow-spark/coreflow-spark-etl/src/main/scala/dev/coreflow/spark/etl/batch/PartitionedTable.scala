package dev.coreflow.spark.etl.batch

import dev.coreflow.spark.etl.batch.params.ReadPartitionParams

import scala.collection.immutable.HashSet

/**
 * <PartitionedTable> is a trait that represents a partitioned Spark table.
 */
trait PartitionedTable[P <: ReadPartitionParams] extends ReadableTable {
  def partitionColumns: HashSet[String]

  /**
   * @param columns The columns to check.
   * @return True if the columns are a subset of the partition columns.
   */
  def isPartitionSubset(columns: Set[String]): Boolean = {
    columns.subsetOf(partitionColumns)
  }
}
