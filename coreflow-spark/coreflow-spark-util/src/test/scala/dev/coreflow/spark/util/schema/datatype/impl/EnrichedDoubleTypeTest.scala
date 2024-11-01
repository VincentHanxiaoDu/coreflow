package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.exceptions.ConversionException
import org.apache.spark.sql.types.DoubleType
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class EnrichedDoubleTypeTest {

  @Test
  def testTypeCompatibility(): Unit = {
    assertEquals(4, EnrichedDoubleType.typeCompatibility, "Expected type compatibility level 4 " +
      "for EnrichedDoubleType.")
  }

  @Test
  def testToSparkDataType(): Unit = {
    assertEquals(DoubleType, EnrichedDoubleType.toSparkDataType, "Expected Spark DoubleType for " +
      "EnrichedDoubleType.")
  }

  @Test
  def testConvertValueToCompatibleWithDouble(): Unit = {
    val value = 123.45
    val convertedValue = EnrichedDoubleType.convertValueToCompatible(value,
      ignoreConversionErrors = false)
    assertEquals(value, convertedValue, "Expected Double value to be converted as-is.")
  }

  @Test
  def testConvertValueToCompatibleWithLong(): Unit = {
    val value = 123L
    val convertedValue = EnrichedDoubleType.convertValueToCompatible(value,
      ignoreConversionErrors = false)
    assertEquals(value.toDouble, convertedValue, "Expected Long value to be converted to Double.")
  }

  @Test
  def testConvertValueToCompatibleWithInt(): Unit = {
    val value = 123
    val convertedValue = EnrichedDoubleType.convertValueToCompatible(value,
      ignoreConversionErrors = false)
    assertEquals(value.toDouble, convertedValue, "Expected Int value to be converted to Double.")
  }

  @Test
  def testConvertValueToCompatibleWithBoolean(): Unit = {
    val convertedTrue = EnrichedDoubleType.convertValueToCompatible(true, ignoreConversionErrors
    = false)
    assertEquals(1D, convertedTrue, "Expected Boolean true to be converted to Double 1.0.")

    val convertedFalse = EnrichedDoubleType.convertValueToCompatible(false,
      ignoreConversionErrors = false)
    assertEquals(0D, convertedFalse, "Expected Boolean false to be converted to Double 0.0.")
  }

  @Test
  def testConvertValueToCompatibleWithNonConvertibleType(): Unit = {
    assertThrows(
      classOf[ConversionException],
      () => EnrichedDoubleType.convertValueToCompatible("notANumber", ignoreConversionErrors =
        false)
    )
  }

  @Test
  def testConvertValueToCompatibleWithNonConvertibleTypeIgnoreErrorsTrue(): Unit = {
    val result = EnrichedDoubleType.convertValueToCompatible("notANumber", ignoreConversionErrors
    = true)
    assertNull(result, "Expected null when converting non-numeric value with " +
      "ignoreConversionErrors = true.")
  }

  @Test
  def testConvertValueToCompatibleWithNullValue(): Unit = {
    val result = EnrichedDoubleType.convertValueToCompatible(null, ignoreConversionErrors = false)
    assertNull(result, "Expected null when converting a null value.")
  }
}

