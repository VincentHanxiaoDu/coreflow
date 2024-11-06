package dev.coreflow.spark.etl.batch

import org.apache.spark.sql.SparkSession

/**
 * <ReadableTable> is a trait that represents a readable Spark table.
 *
 * @tparam O The output type of the table.
 */
trait ReadableTable[O] extends LogicalTable {
  /**
   * @param spark The SparkSession.
   * @return The DataFrame that represents the table.
   */
  def readTable(implicit spark: SparkSession): O
}
