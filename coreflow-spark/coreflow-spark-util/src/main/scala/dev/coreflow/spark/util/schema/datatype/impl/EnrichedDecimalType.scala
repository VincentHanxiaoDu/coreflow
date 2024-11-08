package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.EnrichedDataType
import org.apache.spark.sql.types.DecimalType

import scala.collection.mutable

/**
 * Enriched data type for decimal values.
 * @param precision The precision of the decimal.
 * @param scale The scale of the decimal.
 */
@SerialVersionUID(1728954167345109600L)
class EnrichedDecimalType(val precision: Int, val scale: Int) extends EnrichedDataType {

  final override def typeCompatibility: Int = 5

  override def toSparkDataType: DecimalType = DecimalType(precision, scale)

  override def convertValueToCompatible(value: Any, ignoreConversionErrors: Boolean): Any = {
    Option(value).flatMap[BigDecimal] {
      case d: BigDecimal => Some(d)
      case d: Double => Some(BigDecimal(d))
      case l: Long => Some(BigDecimal(l))
      case i: Int => Some(BigDecimal(i))
      case b: Boolean => Some(BigDecimal(if (b) 1 else 0))
      case _ => handleConversionError(value, ignoreConversionErrors)
    }.orNull
  }

  override def reduceToCompatibleDataType(that: EnrichedDataType): EnrichedDataType = {
    that match {
      case d: DecimalType if this == d => this
      case d: DecimalType =>
        EnrichedDecimalType(this.precision max d.precision, this.scale max d.scale)
      case _ => super.reduceToCompatibleDataType(that)
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: EnrichedDecimalType => this.precision == other.precision &&
      this.scale == other.scale
    case _ => false
  }

  override def hashCode(): Int = (precision, scale).##
}

object EnrichedDecimalType {
  private val instances = mutable.HashMap[(Int, Int), EnrichedDecimalType]()

  def apply(precision: Int, scale: Int): EnrichedDecimalType = {
    instances.getOrElseUpdate((precision, scale), new EnrichedDecimalType(precision, scale))
  }

  def apply(sparkDecimalType: DecimalType): EnrichedDecimalType = {
    EnrichedDecimalType(sparkDecimalType.precision, sparkDecimalType.scale)
  }
}
