package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.EnrichedDataType
import org.apache.spark.sql.types.LongType

/**
 * Enriched data type for long values.
 */
@SerialVersionUID(623471457155526542L)
object EnrichedLongType extends EnrichedDataType {
  final override def typeCompatibility: Int = 3

  override def toSparkDataType: LongType = LongType

  override def convertValueToCompatible(value: Any, ignoreConversionErrors: Boolean): Any = {
    Option(value).flatMap[Long] {
      case l: Long => Some(l)
      case i: Int => Some(i.toLong)
      case b: Boolean => Some(if (b) 1L else 0L)
      case _ => handleConversionError(value, ignoreConversionErrors)
    }.orNull
  }
}
