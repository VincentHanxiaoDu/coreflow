package dev.coreflow.spark.etl.batch

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * <ReadableTable> is a trait that represents a readable Spark table.
 */
trait ReadableTable extends LogicalTable {
  /**
   * @param spark The SparkSession.
   * @return The DataFrame that represents the table.
   */
  def readTable(implicit spark: SparkSession): DataFrame
}
