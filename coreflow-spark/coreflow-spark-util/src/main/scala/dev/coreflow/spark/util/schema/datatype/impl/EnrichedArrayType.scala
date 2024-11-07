package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.EnrichedDataType
import org.apache.spark.sql.types.ArrayType

import scala.collection.mutable

/**
 * Enriched data type for array values.
 *
 * @param elementDataType The enriched type for elements of the array.
 */
@SerialVersionUID(4714343744563410534L)
class EnrichedArrayType(val elementDataType: EnrichedDataType) extends EnrichedDataType {

  final override def typeCompatibility: Int = 7

  override def toSparkDataType: ArrayType = ArrayType(elementDataType.toSparkDataType)

  override def convertValueToCompatible(value: Any, ignoreConversionErrors: Boolean): Any = {
    Option(value).flatMap[Seq[_]] {
      case a: Seq[_] => Some(
        a.map(elementDataType.convertValueToCompatible(_, ignoreConversionErrors)))
      case _ => handleConversionError(value, ignoreConversionErrors)
    }.orNull
  }

  override def reduceToCompatibleDataType(that: EnrichedDataType): EnrichedDataType = {
    that match {
      case a: EnrichedArrayType =>
        EnrichedArrayType(elementDataType.reduceToCompatibleDataType(a.elementDataType))
      case _ => super.reduceToCompatibleDataType(that)
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case a: EnrichedArrayType => this.elementDataType == a.elementDataType
    case _ => false
  }

  override def hashCode(): Int = elementDataType.##
}

object EnrichedArrayType {
  private val instances = mutable.HashMap[EnrichedDataType, EnrichedArrayType]()

  def apply(elementDataType: EnrichedDataType): EnrichedArrayType = {
    instances.getOrElseUpdate(elementDataType, new EnrichedArrayType(elementDataType))
  }

  def apply(arrayDataType: ArrayType): EnrichedArrayType = {
    EnrichedArrayType(EnrichedDataType.fromSparkDataType(arrayDataType.elementType))
  }
}
