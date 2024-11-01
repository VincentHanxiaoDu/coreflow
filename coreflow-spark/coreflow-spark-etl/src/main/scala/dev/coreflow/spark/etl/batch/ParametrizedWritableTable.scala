package dev.coreflow.spark.etl.batch

import dev.coreflow.spark.etl.params.Params
import org.apache.spark.sql.SparkSession

/**
 * Writable Spark table with some parameters for writing.
 */
trait ParametrizedWritableTable[P <: Params] extends MaterializedTable {
  /**
   * @param params The parameters to write the table.
   * @param spark  The SparkSession.
   */
  def writeFull(params: P)(implicit spark: SparkSession): Unit
}
