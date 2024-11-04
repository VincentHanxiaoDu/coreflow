package dev.coreflow.spark.etl.batch

import org.apache.spark.sql.SparkSession


/**
 * <WritableTable> is a trait that represents a writable Spark table.
 */
trait WritableTable extends MaterializedTable {

  /**
   * @param spark The SparkSession.
   */
  def writeFull()(implicit spark: SparkSession): Unit
}
