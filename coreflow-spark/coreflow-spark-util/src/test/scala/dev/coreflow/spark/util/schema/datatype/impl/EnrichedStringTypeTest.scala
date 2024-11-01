package dev.coreflow.spark.util.schema.datatype.impl

import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class EnrichedStringTypeTest {

  @Test
  def testTypeCompatibility(): Unit = {
    assertEquals(6, EnrichedStringType.typeCompatibility, "Expected type compatibility to be 6")
  }

  @Test
  def testToSparkDataType(): Unit = {
    assertEquals(StringType, EnrichedStringType.toSparkDataType, "Expected Spark data type to be " +
      "StringType")
  }

  @Test
  def testConvertValueToCompatibleWithString(): Unit = {
    val result = EnrichedStringType.convertValueToCompatible("test string",
      ignoreConversionErrors = false)
    assertEquals("test string", result, "Expected conversion result to be the same as input string")
  }

  @Test
  def testConvertValueToCompatibleWithNonString(): Unit = {
    val result = EnrichedStringType.convertValueToCompatible(42, ignoreConversionErrors = false)
    assertEquals("42", result, "Expected conversion result to be '42' for integer input")
  }

  @Test
  def testConvertValueToCompatibleWithNull(): Unit = {
    val result = EnrichedStringType.convertValueToCompatible(null, ignoreConversionErrors = false)
    assertNull(result, "Expected conversion result to be null for null input")
  }

  @Test
  def testConvertValueToCompatibleWithBoolean(): Unit = {
    val trueResult = EnrichedStringType.convertValueToCompatible(true, ignoreConversionErrors =
      false)
    assertEquals("true", trueResult, "Expected conversion result to be 'true' for boolean input " +
      "true")

    val falseResult = EnrichedStringType.convertValueToCompatible(false, ignoreConversionErrors =
      false)
    assertEquals("false", falseResult, "Expected conversion result to be 'false' for boolean " +
      "input false")
  }
}

