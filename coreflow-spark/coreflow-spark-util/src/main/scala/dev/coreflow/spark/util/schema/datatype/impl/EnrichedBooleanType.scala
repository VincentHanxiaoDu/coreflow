package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.EnrichedDataType
import org.apache.spark.sql.types.BooleanType

/**
 * Enriched data type for boolean values.
 */
@SerialVersionUID(1164429050839663293L)
object EnrichedBooleanType extends EnrichedDataType {
  final override def typeCompatibility: Int = 1

  override def toSparkDataType: BooleanType = BooleanType

  override def convertValueToCompatible(value: Any, ignoreConversionErrors: Boolean): Any = {
    Option(value).flatMap[Boolean] {
      case b: Boolean => Some(b)
      case _ => handleConversionError(value, ignoreConversionErrors)
    }.orNull
  }
}
