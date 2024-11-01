package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.EnrichedDataType
import org.apache.spark.sql.types.StringType

/**
 * Enriched data type for string values.
 */
@SerialVersionUID(8462419348864407224L)
object EnrichedStringType extends EnrichedDataType {
  final override def typeCompatibility: Int = 6

  override def toSparkDataType: StringType = StringType

  override def convertValueToCompatible(value: Any, ignoreConversionErrors: Boolean): String = {
    Option(value).flatMap[String] {
      case s: String => Some(s)
      case a => Some(a.toString)
    }.orNull
  }
}
