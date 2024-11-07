package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.EnrichedDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.immutable.ListMap
import scala.collection.mutable

/**
 * Enriched data type for struct values.
 *
 * @param internalDataTypes The data types of the struct fields.
 */
@SerialVersionUID(3693789401695146023L)
class EnrichedStructType(val internalDataTypes: ListMap[String, EnrichedDataType])
  extends EnrichedDataType {
  final override def typeCompatibility: Int = 8

  override def toSparkDataType: StructType = StructType(
    internalDataTypes.map {
      case (k, v) => StructField(k, v.toSparkDataType)
    }.toArray
  )

  override def convertValueToCompatible(value: Any, ignoreConversionErrors: Boolean): Row = {

    val valueMap = Option(value).flatMap {
      case map: Map[_, _] => Option(map.map {
        case (k, v) => k.toString -> v
      })
      case _ => handleConversionError(value, ignoreConversionErrors)
    }.getOrElse(Map.empty)

    val values = internalDataTypes.map {
      case (k, valueDataType) => valueMap.get(k).map(
        valueDataType.convertValueToCompatible(_, ignoreConversionErrors)).orNull
    }.toSeq
    Row.fromSeq(values)
  }

  override def reduceToCompatibleDataType(that: EnrichedDataType): EnrichedDataType = {
    that match {
      case m: EnrichedStructType =>
        val internalDataTypesUpdated = internalDataTypes.map {
          case (k, valueDataType) => k -> m.internalDataTypes.get(k)
            .map(valueDataType.reduceToCompatibleDataType).getOrElse(valueDataType)
        }
        val extraDataTypes = ListMap(m.internalDataTypes
          .filterKeys(!internalDataTypes.contains(_)).toSeq: _*)
        EnrichedStructType(internalDataTypesUpdated ++ extraDataTypes)
      case _ => super.reduceToCompatibleDataType(that)
    }
  }
}

object EnrichedStructType {
  private val instances = mutable.HashMap[ListMap[String, EnrichedDataType], EnrichedStructType]()

  def apply(internalDataTypes: ListMap[String, EnrichedDataType]): EnrichedStructType = {
    instances.getOrElseUpdate(internalDataTypes, new EnrichedStructType(internalDataTypes))
  }

  def apply(internalDataTypes: Map[String, EnrichedDataType]): EnrichedStructType = {
    apply(ListMap(internalDataTypes.toSeq: _*))
  }

  def apply(structDataType: StructType): EnrichedStructType = {
    EnrichedStructType(
      ListMap(structDataType.map {
        case StructField(name, dataType, _, _) => name -> EnrichedDataType.fromSparkDataType(dataType)
      }: _*)
    )
  }
}