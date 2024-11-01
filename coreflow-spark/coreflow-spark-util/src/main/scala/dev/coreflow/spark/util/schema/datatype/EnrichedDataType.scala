package dev.coreflow.spark.util.schema.datatype

import dev.coreflow.spark.util.schema.datatype.exceptions._
import dev.coreflow.spark.util.schema.datatype.impl._
import org.apache.spark.sql.types._

import scala.collection.immutable.ListMap

/**
 * Enriched data type for finding suitable data types for Spark dataframes.
 */
trait EnrichedDataType extends Ordered[EnrichedDataType] with Serializable {
  /**
   * @return The compatibility of this data type.
   */
  protected def typeCompatibility: Int

  /**
   * @return The Spark data type representation of this data type.
   */
  def toSparkDataType: DataType

  /**
   * @param that The other data type to compare to.
   * @return The result of the comparison.
   */
  override def compare(that: EnrichedDataType): Int = {
    this.typeCompatibility.compare(that.typeCompatibility)
  }

  /**
   * @param value                  The value to convert.
   * @param ignoreConversionErrors Whether to ignore conversion errors.
   * @return The converted value.
   */
  def convertValueToCompatible(value: Any, ignoreConversionErrors: Boolean): Any

  /**
   * @param that The other data type to reduce to.
   * @return The reduced data type, which is compatible with both data types.
   */
  def reduceToCompatibleDataType(that: EnrichedDataType): EnrichedDataType = {
    if (this <= that) that else this
  }

  /**
   * @param value                  The value to convert.
   * @param ignoreConversionErrors Whether to ignore conversion errors.
   * @return None if the value is null.
   * @throws ConversionException if the value is not null and conversion errors are not ignored.
   */
  protected def handleConversionError(value: Any, ignoreConversionErrors: Boolean): None.type = {
    if (!ignoreConversionErrors) {
      throw ConversionException(value, this)
    }
    None
  }
}

object EnrichedDataType {

  final val minPreferenceDataType: EnrichedDataType = EnrichedBooleanType

  /**
   * @param m The map value to handle.
   * @return The enriched struct type inferred from the map.
   */
  private def handleStructType(m: Map[String, _]): EnrichedStructType = {
    EnrichedStructType(
      m.map {
        case (k, v) => k -> fromValue(v)
      }
    )
  }

  /**
   * @param m The map value to handle.
   * @return The enriched map type inferred from the map.
   */
  private def handleMapType(m: Map[_, _]): EnrichedMapType = {
    val keyType = m.keys.map(fromValue).reduce(_ reduceToCompatibleDataType _)
    val valueType = m.values.map(fromValue).reduce(_ reduceToCompatibleDataType _)
    EnrichedMapType(keyType, valueType)
  }

  /**
   * @param a The array value to handle.
   * @return The enriched array type inferred from the array.
   */
  private def handleArrayType(a: Seq[_]): EnrichedArrayType = {
    val elementDataType = a.map(fromValue).reduce(_ reduceToCompatibleDataType _)
    EnrichedArrayType(elementDataType)
  }

  /**
   * @param value The value to infer the enriched data type from.
   * @return The enriched data type inferred from the value.
   */
  def fromValue(value: Any): EnrichedDataType = {
    Option(value).map {
      case _: Boolean => EnrichedBooleanType
      case _: Int => EnrichedIntegerType
      case _: Long => EnrichedLongType
      case _: Double => EnrichedDoubleType
      case _: String => EnrichedStringType
      case d: BigDecimal => EnrichedDecimalType(d.precision, d.scale)
      case m: Map[_, _] =>
        val maybeStringKeyMap = m.foldLeft(Option(Map.empty[String, Any])) {
          case (Some(acc), (k: String, v)) => Some(acc + (k -> v))
          case _ => None
        }
        maybeStringKeyMap.map {
          stringKeyMap => handleStructType(stringKeyMap)
        }.getOrElse(handleMapType(m))
      case s: Seq[_] => handleArrayType(s)
      case a: Array[_] => handleArrayType(a.toSeq)
    }.getOrElse(minPreferenceDataType)
  }


  /**
   * @param dataType The Spark data type to convert.
   * @return The enriched data type representation of the Spark data type.
   */
  def fromSparkDataType(dataType: DataType): EnrichedDataType = {
    dataType match {
      case BooleanType => EnrichedBooleanType
      case IntegerType => EnrichedIntegerType
      case LongType => EnrichedLongType
      case DoubleType => EnrichedDoubleType
      case StringType => EnrichedStringType
      case d: DecimalType => EnrichedDecimalType(d.precision, d.scale)
      case a: ArrayType =>
        EnrichedArrayType(fromSparkDataType(a.elementType))
      case m: MapType =>
        EnrichedMapType(fromSparkDataType(m.keyType), fromSparkDataType(m.valueType))
      case s: StructType =>
        EnrichedStructType(ListMap(s.fields.map(field => field.name ->
          fromSparkDataType(field.dataType)): _*))
      case _ => throw UnsupportedDataTypeException(dataType)
    }
  }
}
