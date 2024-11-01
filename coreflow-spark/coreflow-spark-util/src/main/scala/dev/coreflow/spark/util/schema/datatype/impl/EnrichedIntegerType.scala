package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.EnrichedDataType
import org.apache.spark.sql.types.IntegerType

/**
 * Enriched data type for integer values.
 */
@SerialVersionUID(2332005992000982819L)
object EnrichedIntegerType extends EnrichedDataType {
  final override def typeCompatibility: Int = 2

  override def toSparkDataType: IntegerType = IntegerType

  override def convertValueToCompatible(value: Any, ignoreConversionErrors: Boolean): Any = {
    Option(value).flatMap[Int] {
      case i: Int => Some(i)
      case b: Boolean => Some(if (b) 1 else 0)
      case _ => handleConversionError(value, ignoreConversionErrors)
    }.orNull
  }
}