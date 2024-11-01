package dev.coreflow.spark.util.schema.datatype.exceptions

import dev.coreflow.spark.util.schema.datatype.EnrichedDataType

/**
 * @param message THe conversion exception message.
 */
case class ConversionException(message: String) extends Exception(message)

object ConversionException {

  /**
   * @param value            The value that cannot be converted.
   * @param expectedDataType The expected data type.
   * @return the constructed ConversionException.
   */
  def apply(value: Any, expectedDataType: EnrichedDataType): ConversionException = {
    new ConversionException(
      Option(value).map {
        _ => s"Value of type `${value.getClass}` cannot be converted to `${expectedDataType
          .getClass}`"
      }.getOrElse(s"Unexpected value: null for `${expectedDataType.getClass}`")
    )
  }
}
