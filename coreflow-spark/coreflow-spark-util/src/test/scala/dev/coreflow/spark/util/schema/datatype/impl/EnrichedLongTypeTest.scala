package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.exceptions.ConversionException
import org.apache.spark.sql.types.LongType
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class EnrichedLongTypeTest {

  @Test
  def testTypeCompatibility(): Unit = {
    assertEquals(3, EnrichedLongType.typeCompatibility, "Expected type compatibility to be 3")
  }

  @Test
  def testToSparkDataType(): Unit = {
    assertEquals(LongType, EnrichedLongType.toSparkDataType, "Expected Spark data type to be LongType")
  }

  @Test
  def testConvertValueToCompatibleWithLong(): Unit = {
    val result = EnrichedLongType.convertValueToCompatible(42L, ignoreConversionErrors = false)
    assertEquals(42L, result, "Expected conversion result to be 42L for long input")
  }

  @Test
  def testConvertValueToCompatibleWithInt(): Unit = {
    val result = EnrichedLongType.convertValueToCompatible(42, ignoreConversionErrors = false)
    assertEquals(42L, result, "Expected conversion result to be 42L for integer input")
  }

  @Test
  def testConvertValueToCompatibleWithBoolean(): Unit = {
    val trueResult = EnrichedLongType.convertValueToCompatible(true, ignoreConversionErrors = false)
    assertEquals(1L, trueResult, "Expected conversion result to be 1L for boolean input 'true'")

    val falseResult = EnrichedLongType.convertValueToCompatible(false, ignoreConversionErrors = false)
    assertEquals(0L, falseResult, "Expected conversion result to be 0L for boolean input 'false'")
  }

  @Test
  def testConvertValueToCompatibleWithInvalidTypeAndIgnoreErrors(): Unit = {
    val result = EnrichedLongType.convertValueToCompatible("invalid", ignoreConversionErrors = true)
    assertNull(result, "Expected result to be null when ignoring conversion errors for invalid input")
  }

  @Test
  def testConvertValueToCompatibleWithInvalidTypeAndThrowError(): Unit = {
    assertThrows(classOf[ConversionException], () => {
      EnrichedLongType.convertValueToCompatible("invalid", ignoreConversionErrors = false)
    })
  }
}

