package dev.coreflow.spark.util.schema.generator

import dev.coreflow.spark.util.schema.datatype.impl.EnrichedStructType
import org.apache.spark.rdd.RDD

/**
 * A schema generator is responsible for inferring the schema of a dataset.
 */
trait SchemaGenerator {
  /**
   * @param rdd           The RDD to infer the schema from.
   * @param samplingRatio The fraction of the data to use for schema inference.
   * @param seed          The seed for the random number generator.
   * @return
   */
  def getInferredSchema(rdd: RDD[Map[String, _]], samplingRatio: Double, seed: Long = 0L)
  : EnrichedStructType = {
    require(samplingRatio > 0 && samplingRatio < 1, "Sampling ratio must be in (0, 1).")
    getInferredSchema(rdd.sample(withReplacement = false, samplingRatio, seed))
  }

  /**
   * @param rdd The RDD to infer the schema from.
   * @return The inferred schema.
   */
  def getInferredSchema(rdd: RDD[Map[String, _]]): EnrichedStructType
}
