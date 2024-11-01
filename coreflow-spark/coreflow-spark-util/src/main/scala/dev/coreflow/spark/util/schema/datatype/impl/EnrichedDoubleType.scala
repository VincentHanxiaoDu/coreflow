package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.EnrichedDataType
import org.apache.spark.sql.types.DoubleType

/**
 * Enriched data type for double values.
 */
@SerialVersionUID(6312896149045933059L)
object EnrichedDoubleType extends EnrichedDataType {
  final override def typeCompatibility: Int = 4

  override def toSparkDataType: DoubleType = DoubleType

  override def convertValueToCompatible(value: Any, ignoreConversionErrors: Boolean): Any = {
    Option(value).flatMap[Double] {
      case d: Double => Some(d)
      case l: Long => Some(l.toDouble)
      case i: Int => Some(i.toDouble)
      case b: Boolean => Some(if (b) 1D else 0D)
      case _ => handleConversionError(value, ignoreConversionErrors)
    }.orNull
  }
}
