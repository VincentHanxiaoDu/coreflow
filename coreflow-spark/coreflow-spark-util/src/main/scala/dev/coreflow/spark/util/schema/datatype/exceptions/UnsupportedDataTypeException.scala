package dev.coreflow.spark.util.schema.datatype.exceptions

import org.apache.spark.sql.types.DataType

/**
 * @param message The exception message.
 */
case class UnsupportedDataTypeException(message: String) extends Exception(message)

object UnsupportedDataTypeException {
  def apply(dataType: DataType): UnsupportedDataTypeException = {
    new UnsupportedDataTypeException(
      s"Unsupported data type: `${dataType.getClass}`"
    )
  }
}

