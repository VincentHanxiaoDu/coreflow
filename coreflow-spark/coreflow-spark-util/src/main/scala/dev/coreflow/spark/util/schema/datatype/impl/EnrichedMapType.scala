package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.EnrichedDataType
import org.apache.spark.sql.types.MapType

import scala.collection.mutable

/**
 * Enriched data type for map values.
 *
 * @param keyType   The data type of the map keys.
 * @param valueType The data type of the map values.
 */
@SerialVersionUID(5313309701105084002L)
class EnrichedMapType(val keyType: EnrichedDataType, val valueType: EnrichedDataType) extends
  EnrichedDataType {

  final override def typeCompatibility: Int = 9

  override def toSparkDataType: MapType = MapType(keyType.toSparkDataType, valueType
    .toSparkDataType)

  override def convertValueToCompatible(value: Any, ignoreConversionErrors: Boolean): Any = {
    Option(value).flatMap[Map[_, _]] {
      case m: Map[_, _] =>
        Some(
          m.map {
            case (k, v) => keyType.convertValueToCompatible(k, ignoreConversionErrors) ->
              valueType.convertValueToCompatible(v, ignoreConversionErrors)
          }
        )
      case _ => handleConversionError(value, ignoreConversionErrors)
    }.orNull
  }

  override def reduceToCompatibleDataType(that: EnrichedDataType): EnrichedDataType = {
    that match {
      case m: EnrichedMapType if this == m => this
      case m: EnrichedMapType =>
        EnrichedMapType(
          keyType.reduceToCompatibleDataType(m.keyType),
          valueType.reduceToCompatibleDataType(m.valueType)
        )
      case _ => super.reduceToCompatibleDataType(that)
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case m: EnrichedMapType => keyType == m.keyType && valueType == m.valueType
    case _ => false
  }

  override def hashCode(): Int = (keyType, valueType).##
}

object EnrichedMapType {
  private val instances = mutable.HashMap[(EnrichedDataType, EnrichedDataType), EnrichedMapType]()

  def apply(keyType: EnrichedDataType, valueType: EnrichedDataType): EnrichedMapType = {
    instances.getOrElseUpdate((keyType, valueType), new EnrichedMapType(keyType, valueType))
  }

  def apply(mapDataType: MapType): EnrichedMapType = {
    EnrichedMapType(
      EnrichedDataType.fromSparkDataType(mapDataType.keyType),
      EnrichedDataType.fromSparkDataType(mapDataType.valueType)
    )
  }
}
