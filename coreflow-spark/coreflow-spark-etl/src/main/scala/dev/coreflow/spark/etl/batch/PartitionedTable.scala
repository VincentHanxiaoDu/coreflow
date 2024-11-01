package dev.coreflow.spark.etl.batch

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.immutable.HashSet

/**
 * <PartitionedTable> is a trait that represents a partitioned Spark table.
 */
trait PartitionedTable extends ReadableTable {
  def partitionColumns: HashSet[String]

  /**
   * @param columns The columns to check.
   * @return True if the columns are a subset of the partition columns.
   */
  def isPartitionSubset(columns: Set[String]): Boolean = {
    columns.subsetOf(partitionColumns)
  }

  /**
   * @param filterColumn The filter column.
   * @param spark        The SparkSession.
   * @return The DataFrame that represents the table with the filter column.
   */
  def readPartitions(filterColumn: Column)(implicit spark: SparkSession): DataFrame = {
    readTable().filter(filterColumn)
  }
}
