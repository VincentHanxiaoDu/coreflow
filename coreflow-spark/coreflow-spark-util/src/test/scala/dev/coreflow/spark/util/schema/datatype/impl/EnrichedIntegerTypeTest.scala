package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.exceptions.ConversionException
import org.apache.spark.sql.types.IntegerType
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable

class EnrichedIntegerTypeTest {

  @Test
  def testTypeCompatibility(): Unit = {
    assertEquals(2, EnrichedIntegerType.typeCompatibility, "Expected type compatibility to be 2")
  }

  @Test
  def testToSparkDataType(): Unit = {
    assertEquals(IntegerType, EnrichedIntegerType.toSparkDataType, "Expected Spark data type to " +
      "be IntegerType")
  }

  @Test
  def testConvertValueToCompatibleWithInt(): Unit = {
    val result = EnrichedIntegerType.convertValueToCompatible(42, ignoreConversionErrors = false)
    assertEquals(42, result, "Expected conversion result to be 42 for integer input")
  }

  @Test
  def testConvertValueToCompatibleWithBoolean(): Unit = {
    val trueResult = EnrichedIntegerType.convertValueToCompatible(true, ignoreConversionErrors =
      false)
    assertEquals(1, trueResult, "Expected conversion result to be 1 for boolean input 'true'")

    val falseResult = EnrichedIntegerType.convertValueToCompatible(false, ignoreConversionErrors
    = false)
    assertEquals(0, falseResult, "Expected conversion result to be 0 for boolean input 'false'")
  }

  @Test
  def testConvertValueToCompatibleWithInvalidTypeAndIgnoreErrors(): Unit = {
    val result = EnrichedIntegerType.convertValueToCompatible("invalid", ignoreConversionErrors =
      true)
    assertNull(result, "Expected result to be null when ignoring conversion errors for invalid " +
      "input")
  }

  @Test
  def testConvertValueToCompatibleWithInvalidTypeAndThrowError(): Unit = {
    assertThrows(
      classOf[ConversionException],
      new Executable {
        override def execute(): Unit = {
          EnrichedIntegerType.convertValueToCompatible("invalid", ignoreConversionErrors = false)
        }
      },
      "Expected ConversionException when converting non-numeric value with ignoreConversionErrors" +
        " = false."
    )
  }
}
