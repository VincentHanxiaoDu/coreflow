package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.exceptions.ConversionException
import org.apache.spark.sql.types.DecimalType
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class EnrichedDecimalTypeTest {

  @Test
  def testTypeCompatibility(): Unit = {
    val decimalType = EnrichedDecimalType(10, 2)
    assertEquals(5, decimalType.typeCompatibility, "Expected type compatibility level 5 for " +
      "EnrichedDecimalType.")
  }

  @Test
  def testToSparkDataType(): Unit = {
    val decimalType = EnrichedDecimalType(10, 2)
    assertEquals(DecimalType(10, 2), decimalType.toSparkDataType, "Expected Spark DecimalType(10," +
      " 2) for EnrichedDecimalType.")
  }

  @Test
  def testConvertValueToCompatibleWithBigDecimal(): Unit = {
    val decimalType = EnrichedDecimalType(10, 2)
    val value = BigDecimal("123.45")
    val convertedValue = decimalType.convertValueToCompatible(value, ignoreConversionErrors = false)
    assertEquals(value, convertedValue, "Expected BigDecimal value to be converted as-is.")
  }

  @Test
  def testConvertValueToCompatibleWithDouble(): Unit = {
    val decimalType = EnrichedDecimalType(10, 2)
    val value = 123.45
    val convertedValue = decimalType.convertValueToCompatible(value, ignoreConversionErrors = false)
    assertEquals(BigDecimal(value), convertedValue, "Expected Double value to be converted to " +
      "BigDecimal.")
  }

  @Test
  def testConvertValueToCompatibleWithLong(): Unit = {
    val decimalType = EnrichedDecimalType(10, 2)
    val value = 123L
    val convertedValue = decimalType.convertValueToCompatible(value, ignoreConversionErrors = false)
    assertEquals(BigDecimal(value), convertedValue, "Expected Long value to be converted to " +
      "BigDecimal.")
  }

  @Test
  def testConvertValueToCompatibleWithInt(): Unit = {
    val decimalType = EnrichedDecimalType(10, 2)
    val value = 123
    val convertedValue = decimalType.convertValueToCompatible(value, ignoreConversionErrors = false)
    assertEquals(BigDecimal(value), convertedValue, "Expected Int value to be converted to " +
      "BigDecimal.")
  }

  @Test
  def testConvertValueToCompatibleWithBoolean(): Unit = {
    val decimalType = EnrichedDecimalType(10, 2)
    val convertedTrue = decimalType.convertValueToCompatible(true, ignoreConversionErrors = false)
    assertEquals(BigDecimal(1), convertedTrue, "Expected Boolean true to be converted to " +
      "BigDecimal(1).")

    val convertedFalse = decimalType.convertValueToCompatible(false, ignoreConversionErrors = false)
    assertEquals(BigDecimal(0), convertedFalse, "Expected Boolean false to be converted to " +
      "BigDecimal(0).")
  }

  @Test
  def testConvertValueToCompatibleWithNonConvertibleType(): Unit = {
    val decimalType = EnrichedDecimalType(10, 2)
    assertThrows(
      classOf[ConversionException],
      () => decimalType.convertValueToCompatible("notANumber", ignoreConversionErrors = false)
    )
  }

  @Test
  def testReduceToCompatibleDataType(): Unit = {
    val decimalType1 = EnrichedDecimalType(10, 2)
    val decimalType2 = EnrichedDecimalType(15, 3)
    val compatibleType = decimalType1.reduceToCompatibleDataType(decimalType2)

    assertTrue(compatibleType.isInstanceOf[EnrichedDecimalType], "Expected result to be an " +
      "EnrichedDecimalType.")
    assertEquals(15, compatibleType.asInstanceOf[EnrichedDecimalType].precision, "Expected " +
      "precision to be the maximum of both precisions (15).")
    assertEquals(3, compatibleType.asInstanceOf[EnrichedDecimalType].scale, "Expected scale to be" +
      " the maximum of both scales (3).")
  }

  @Test
  def testEqualsAndHashCode(): Unit = {
    val decimalType1 = EnrichedDecimalType(10, 2)
    val decimalType2 = EnrichedDecimalType(10, 2)
    val decimalType3 = EnrichedDecimalType(15, 3)

    // Equality
    assertEquals(decimalType1, decimalType2, "Expected two EnrichedDecimalTypes with same " +
      "precision and scale to be equal.")
    assertNotEquals(decimalType1, decimalType3, "Expected EnrichedDecimalTypes with different " +
      "precision or scale to not be equal.")

    // HashCode
    assertEquals(decimalType1.hashCode(), decimalType2.hashCode(), "Expected hash codes to be " +
      "equal for same precision and scale.")
    assertNotEquals(decimalType1.hashCode(), decimalType3.hashCode(), "Expected hash codes to " +
      "differ for different precision or scale.")
  }

  @Test
  def testSingletonPatternInEnrichedDecimalType(): Unit = {
    val decimalType1 = EnrichedDecimalType(10, 2)
    val decimalType2 = EnrichedDecimalType(10, 2)

    assertSame(decimalType1, decimalType2, "Expected the same instance for EnrichedDecimalType " +
      "with same precision and scale.")
  }
}

