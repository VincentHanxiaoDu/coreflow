package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.EnrichedDataType
import org.apache.spark.sql.types.TimestampType

import java.sql.Timestamp
import java.time.Instant

/**
 * Enriched data type for timestamp values.
 */
@SerialVersionUID(81383237463575771L)
object EnrichedTimestampType extends EnrichedDataType {
  final override def typeCompatibility: Int = 10

  override def toSparkDataType: TimestampType = TimestampType

  override def convertValueToCompatible(value: Any, ignoreConversionErrors: Boolean): Timestamp = {
    Option(value).flatMap[Timestamp] {
      case s: Timestamp => Some(s)
      case i: Instant => Some(Timestamp.from(i))
      case _ => handleConversionError(value, ignoreConversionErrors)
    }.orNull
  }
}
