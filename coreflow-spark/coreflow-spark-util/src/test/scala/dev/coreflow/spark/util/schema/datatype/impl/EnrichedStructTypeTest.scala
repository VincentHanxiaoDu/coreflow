package dev.coreflow.spark.util.schema.datatype.impl

import dev.coreflow.spark.util.schema.datatype.exceptions.ConversionException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.immutable.ListMap

class EnrichedStructTypeTest {

  @Test
  def testTypeCompatibility(): Unit = {
    val enrichedStructType = EnrichedStructType(ListMap("field1" -> EnrichedIntegerType, "field2"
      -> EnrichedStringType))
    assertEquals(8, enrichedStructType.typeCompatibility, "Expected type compatibility to be 8")
  }

  @Test
  def testToSparkDataType(): Unit = {
    val enrichedStructType = EnrichedStructType(ListMap("field1" -> EnrichedIntegerType, "field2"
      -> EnrichedStringType))
    val expectedStructType = StructType(Seq(StructField("field1", IntegerType), StructField
    ("field2", StringType)))
    assertEquals(expectedStructType, enrichedStructType.toSparkDataType, "Expected StructType " +
      "with Integer and String fields")
  }

  @Test
  def testConvertValueToCompatibleWithMap(): Unit = {
    val enrichedStructType = EnrichedStructType(ListMap("field1" -> EnrichedIntegerType, "field2"
      -> EnrichedStringType))
    val inputMap = Map("field1" -> 42, "field2" -> "test")
    val result = enrichedStructType.convertValueToCompatible(inputMap, ignoreConversionErrors =
      false)
    val expectedRow = Row(42, "test")
    assertEquals(expectedRow, result, "Expected Row with values (42, 'test')")
  }

  @Test
  def testConvertValueToCompatibleWithMissingFields(): Unit = {
    val enrichedStructType = EnrichedStructType(ListMap("field1" -> EnrichedIntegerType, "field2"
      -> EnrichedStringType))
    val inputMap = Map("field1" -> 42)
    val result = enrichedStructType.convertValueToCompatible(inputMap, ignoreConversionErrors =
      false)
    val expectedRow = Row(42, null)
    assertEquals(expectedRow, result, "Expected Row with values (42, null) for missing field")
  }

  @Test
  def testConvertValueToCompatibleWithInvalidTypeAndIgnoreErrors(): Unit = {
    val enrichedStructType = EnrichedStructType(ListMap("field1" -> EnrichedIntegerType, "field2"
      -> EnrichedStringType))
    val result = enrichedStructType.convertValueToCompatible("invalid", ignoreConversionErrors =
      true)
    assertEquals(Row(null, null), result, "Expected Row with null values when ignoring conversion" +
      " errors")
  }

  @Test
  def testConvertValueToCompatibleWithInvalidTypeAndThrowError(): Unit = {
    val enrichedStructType = EnrichedStructType(ListMap("field1" -> EnrichedIntegerType, "field2"
      -> EnrichedStringType))
    assertThrows(classOf[ConversionException], () => {
      enrichedStructType.convertValueToCompatible("invalid", ignoreConversionErrors = false)
    })
  }

  @Test
  def testReduceToCompatibleDataType(): Unit = {
    val structType1 = EnrichedStructType(ListMap("field1" -> EnrichedIntegerType, "field2" ->
      EnrichedStringType))
    val structType2 = EnrichedStructType(ListMap("field2" -> EnrichedStringType, "field3" ->
      EnrichedIntegerType))

    val reducedType = structType1.reduceToCompatibleDataType(structType2)
      .asInstanceOf[EnrichedStructType]
    val expectedType = EnrichedStructType(ListMap(
      "field1" -> EnrichedIntegerType,
      "field2" -> EnrichedStringType,
      "field3" -> EnrichedIntegerType
    ))
    assertEquals(expectedType, reducedType, "Expected reduced EnrichedStructType with all fields " +
      "from both struct types")
  }

  @Test
  def testEqualsAndHashCode(): Unit = {
    val structType1 = EnrichedStructType(ListMap("field1" -> EnrichedIntegerType, "field2" ->
      EnrichedStringType))
    val structType2 = EnrichedStructType(ListMap("field1" -> EnrichedIntegerType, "field2" ->
      EnrichedStringType))
    val structType3 = EnrichedStructType(ListMap("field1" -> EnrichedStringType, "field2" ->
      EnrichedStringType))

    assertEquals(structType1, structType2, "Expected struct types with the same fields to be equal")
    assertNotEquals(structType1, structType3, "Expected struct types with different fields to not" +
      " be equal")
    assertEquals(structType1.hashCode(), structType2.hashCode(), "Expected equal struct types to " +
      "have the same hash code")
  }
}

