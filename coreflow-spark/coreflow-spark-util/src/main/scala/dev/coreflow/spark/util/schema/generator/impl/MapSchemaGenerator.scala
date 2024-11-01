package dev.coreflow.spark.util.schema.generator.impl

import dev.coreflow.spark.util.schema.datatype.EnrichedDataType
import dev.coreflow.spark.util.schema.datatype.impl.EnrichedStructType
import dev.coreflow.spark.util.schema.generator.SchemaGenerator
import org.apache.spark.rdd.RDD

/**
 * A schema generator that infers the schema from a map of values with column names as keys.
 */
case class MapSchemaGenerator() extends SchemaGenerator {
  override def getInferredSchema(rdd: RDD[Map[String, _]]): EnrichedStructType = {
    rdd.map {
      EnrichedDataType.fromValue
    }.reduce(_ reduceToCompatibleDataType _).asInstanceOf[EnrichedStructType]
  }
}
