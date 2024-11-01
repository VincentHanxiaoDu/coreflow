package dev.coreflow.spark.etl.batch.params

import dev.coreflow.spark.etl.params.Params
import org.apache.spark.sql.Column

/**
 * <ReadPartitionParams> is a trait that represents a set of parameters to read a partition.
 */
trait ReadPartitionParams extends Params {
  /**
   * @return The filter to read the partition.
   */
  def partitionFilter: Column
}
