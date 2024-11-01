package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.exceptions.ConversionException
import org.apache.spark.sql.types.{IntegerType, MapType, StringType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class EnrichedMapTypeTest {

  @Test
  def testTypeCompatibility(): Unit = {
    val enrichedMapType = EnrichedMapType(EnrichedIntegerType, EnrichedStringType)
    assertEquals(9, enrichedMapType.typeCompatibility, "Expected type compatibility to be 9")
  }

  @Test
  def testToSparkDataType(): Unit = {
    val enrichedMapType = EnrichedMapType(EnrichedIntegerType, EnrichedStringType)
    val expectedSparkDataType = MapType(IntegerType, StringType)
    assertEquals(expectedSparkDataType, enrichedMapType.toSparkDataType, "Expected Spark data " +
      "type to be MapType(IntegerType, StringType)")
  }

  @Test
  def testConvertValueToCompatibleWithMap(): Unit = {
    val enrichedMapType = EnrichedMapType(EnrichedIntegerType, EnrichedStringType)
    val mapValue = Map(1 -> "one", 2 -> "two")
    val result = enrichedMapType.convertValueToCompatible(mapValue, ignoreConversionErrors = false)
    assertEquals(mapValue, result, "Expected converted map to be the same as input map")
  }

  @Test
  def testConvertValueToCompatibleWithNestedMap(): Unit = {
    val nestedMapType = EnrichedMapType(EnrichedStringType, EnrichedMapType(EnrichedIntegerType,
      EnrichedStringType))
    val nestedMapValue = Map("key1" -> Map(1 -> "one", 2 -> "two"))
    val result = nestedMapType.convertValueToCompatible(nestedMapValue, ignoreConversionErrors =
      false)
    assertEquals(nestedMapValue, result, "Expected converted nested map to be the same as input " +
      "nested map")
  }

  @Test
  def testConvertValueToCompatibleWithInvalidTypeAndIgnoreErrors(): Unit = {
    val enrichedMapType = EnrichedMapType(EnrichedIntegerType, EnrichedStringType)
    val result = enrichedMapType.convertValueToCompatible("invalid", ignoreConversionErrors = true)
    assertNull(result, "Expected result to be null when ignoring conversion errors for invalid " +
      "input")
  }

  @Test
  def testConvertValueToCompatibleWithInvalidTypeAndThrowError(): Unit = {
    val enrichedMapType = EnrichedMapType(EnrichedIntegerType, EnrichedStringType)
    assertThrows(classOf[ConversionException], () => {
      enrichedMapType.convertValueToCompatible("invalid", ignoreConversionErrors = false)
    })
  }

  @Test
  def testReduceToCompatibleDataType(): Unit = {
    val mapType1 = EnrichedMapType(EnrichedIntegerType, EnrichedStringType)
    val mapType2 = EnrichedMapType(EnrichedIntegerType, EnrichedStringType)
    val reducedType = mapType1.reduceToCompatibleDataType(mapType2)
    assertEquals(mapType1, reducedType, "Expected reduced data type to be the same as original " +
      "map type")
  }

  @Test
  def testEqualsAndHashCode(): Unit = {
    val mapType1 = EnrichedMapType(EnrichedIntegerType, EnrichedStringType)
    val mapType2 = EnrichedMapType(EnrichedIntegerType, EnrichedStringType)
    val mapType3 = EnrichedMapType(EnrichedStringType, EnrichedStringType)

    assertEquals(mapType1, mapType2, "Expected map types with the same key and value types to be " +
      "equal")
    assertNotEquals(mapType1, mapType3, "Expected map types with different key/value types to not" +
      " be equal")
    assertEquals(mapType1.hashCode(), mapType2.hashCode(), "Expected equal map types to have the " +
      "same hash code")
  }
}

