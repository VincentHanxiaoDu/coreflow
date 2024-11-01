package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.exceptions.ConversionException
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class EnrichedArrayTypeTest {

  @Test
  def testTypeCompatibility(): Unit = {
    val intArrayType = EnrichedArrayType(EnrichedIntegerType)
    assertEquals(7, intArrayType.typeCompatibility, "Expected type compatibility level 7 for " +
      "EnrichedArrayType.")
  }

  @Test
  def testToSparkDataType(): Unit = {
    val intArrayType = EnrichedArrayType(EnrichedIntegerType)
    val expectedSparkType = ArrayType(IntegerType)
    assertEquals(expectedSparkType, intArrayType.toSparkDataType, "Expected Spark ArrayType" +
      "(IntegerType).")

    val stringArrayType = EnrichedArrayType(EnrichedStringType)
    val expectedSparkTypeString = ArrayType(StringType)
    assertEquals(expectedSparkTypeString, stringArrayType.toSparkDataType, "Expected Spark " +
      "ArrayType(StringType).")
  }

  @Test
  def testConvertValueToCompatible(): Unit = {
    val intArrayType = EnrichedArrayType(EnrichedIntegerType)

    // Valid conversion
    val validArray = Seq(1, 2, 3)
    val convertedArray = intArrayType.convertValueToCompatible(validArray, ignoreConversionErrors
    = false)
    assertEquals(validArray, convertedArray, "Expected valid integer array to be converted as-is.")

    // Invalid conversion with ignoreConversionErrors = false
    assertThrows(classOf[ConversionException], () => {
      intArrayType.convertValueToCompatible("notAnArray", ignoreConversionErrors = false)
    })

    // Invalid conversion with ignoreConversionErrors = true
    val nullResult = intArrayType.convertValueToCompatible("notAnArray", ignoreConversionErrors =
      true)
    assertNull(nullResult, "Expected null when conversion error is ignored.")
  }

  @Test
  def testReduceToCompatibleDataType(): Unit = {
    val intArrayType = EnrichedArrayType(EnrichedIntegerType)
    val stringArrayType = EnrichedArrayType(EnrichedStringType)
    val compatibleArrayType = intArrayType.reduceToCompatibleDataType(stringArrayType)

    assertTrue(compatibleArrayType.isInstanceOf[EnrichedArrayType], "Expected result to be an " +
      "EnrichedArrayType.")
    assertEquals(EnrichedStringType, compatibleArrayType.asInstanceOf[EnrichedArrayType]
      .elementDataType,
      "Expected element type to be EnrichedStringType as it is more generic.")
  }

  @Test
  def testEqualsAndHashCode(): Unit = {
    val intArrayType1 = EnrichedArrayType(EnrichedIntegerType)
    val intArrayType2 = EnrichedArrayType(EnrichedIntegerType)
    val stringArrayType = EnrichedArrayType(EnrichedStringType)

    // Equality
    assertEquals(intArrayType1, intArrayType2, "Expected two EnrichedArrayTypes with IntegerType " +
      "to be equal.")
    assertNotEquals(intArrayType1, stringArrayType, "Expected EnrichedArrayTypes with different " +
      "element types to not be equal.")

    // HashCode
    assertEquals(intArrayType1.hashCode(), intArrayType2.hashCode(), "Expected hash codes to be " +
      "equal for same element types.")
    assertNotEquals(intArrayType1.hashCode(), stringArrayType.hashCode(), "Expected hash codes to" +
      " differ for different element types.")
  }

  @Test
  def testSingletonPatternInEnrichedArrayType(): Unit = {
    val intArrayType1 = EnrichedArrayType(EnrichedIntegerType)
    val intArrayType2 = EnrichedArrayType(EnrichedIntegerType)

    assertSame(intArrayType1, intArrayType2, "Expected the same instance for EnrichedArrayType " +
      "with IntegerType.")
  }
}
