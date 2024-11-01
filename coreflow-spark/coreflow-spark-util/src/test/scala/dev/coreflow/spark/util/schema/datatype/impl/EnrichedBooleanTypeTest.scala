package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.exceptions.ConversionException
import org.apache.spark.sql.types.BooleanType
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class EnrichedBooleanTypeTest {

  @Test
  def testTypeCompatibility(): Unit = {
    assertEquals(1, EnrichedBooleanType.typeCompatibility, "Expected type compatibility level 1 " +
      "for EnrichedBooleanType.")
  }

  @Test
  def testToSparkDataType(): Unit = {
    assertEquals(BooleanType, EnrichedBooleanType.toSparkDataType, "Expected Spark BooleanType " +
      "for EnrichedBooleanType.")
  }

  @Test
  def testConvertValueToCompatibleWithBoolean(): Unit = {
    val input = true
    val convertedValue = EnrichedBooleanType.convertValueToCompatible(input,
      ignoreConversionErrors = false)
    assertEquals(input, convertedValue, "Expected Boolean value to be converted as-is.")
  }

  @Test
  def testConvertValueToCompatibleWithNonBooleanIgnoreErrorsFalse(): Unit = {
    assertThrows(classOf[ConversionException],
      () => EnrichedBooleanType.convertValueToCompatible("notABoolean", ignoreConversionErrors =
        false)
    )
  }

  @Test
  def testConvertValueToCompatibleWithNonBooleanIgnoreErrorsTrue(): Unit = {
    val result = EnrichedBooleanType.convertValueToCompatible("notABoolean",
      ignoreConversionErrors = true)
    assertNull(result, "Expected null when converting non-Boolean value with " +
      "ignoreConversionErrors = true.")
  }

  @Test
  def testConvertValueToCompatibleWithNullValue(): Unit = {
    val result = EnrichedBooleanType.convertValueToCompatible(null, ignoreConversionErrors = false)
    assertNull(result, "Expected null when converting a null value.")
  }
}
