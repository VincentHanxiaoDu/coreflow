package dev.coreflow.spark.util.schema.datatype

import dev.coreflow.spark.util.schema.datatype.EnrichedDataType.fromSparkDataType
import dev.coreflow.spark.util.schema.datatype.exceptions.UnsupportedDataTypeException
import dev.coreflow.spark.util.schema.datatype.impl._
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.immutable.ListMap

class EnrichedDataTypeTest {

  @Test
  def testFromValueWithBasicTypes(): Unit = {
    // Boolean
    val booleanResult = EnrichedDataType.fromValue(true)
    assertTrue(booleanResult.isInstanceOf[EnrichedBooleanType.type],
      "Expected EnrichedBooleanType for Boolean input.")

    // Integer
    val intResult = EnrichedDataType.fromValue(42)
    assertTrue(intResult.isInstanceOf[EnrichedIntegerType.type],
      "Expected EnrichedIntegerType for Integer input.")

    // Long
    val longResult = EnrichedDataType.fromValue(42L)
    assertTrue(longResult.isInstanceOf[EnrichedLongType.type],
      "Expected EnrichedLongType for Long input.")

    // Double
    val doubleResult = EnrichedDataType.fromValue(42.0)
    assertTrue(doubleResult.isInstanceOf[EnrichedDoubleType.type],
      "Expected EnrichedDoubleType for Double input.")

    // String
    val stringResult = EnrichedDataType.fromValue("hello")
    assertTrue(stringResult.isInstanceOf[EnrichedStringType.type],
      "Expected EnrichedStringType for String input.")

    // BigDecimal
    val bigDecimal = BigDecimal(123.45)
    val decimalResult = EnrichedDataType.fromValue(bigDecimal)
    assertTrue(decimalResult.isInstanceOf[EnrichedDecimalType],
      "Expected EnrichedDecimalType for BigDecimal input.")

    // Additional assertions for precision and scale in BigDecimal
    decimalResult match {
      case decType: EnrichedDecimalType =>
        assertEquals(bigDecimal.precision, decType.precision, "Precision should match.")
        assertEquals(bigDecimal.scale, decType.scale, "Scale should match.")
      case _ => fail("Expected EnrichedDecimalType for BigDecimal input.")
    }
  }


  @Test
  def testFromValueWithStructTypes(): Unit = {
    val structValue = Map("a" -> 1, "b" -> 2.3, "c" -> Seq(1, 2, 3))
    val structResult = EnrichedDataType.fromValue(structValue)

    assertTrue(structResult.isInstanceOf[EnrichedStructType],
      "Expected EnrichedStructType for struct input.")

    val structResultConverted = structResult.asInstanceOf[EnrichedStructType]

    val expectedStructType = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", DoubleType),
      StructField("c", ArrayType(IntegerType))
    ))

    val actualFields = structResultConverted.toSparkDataType.fields.toList
    val expectedFields = expectedStructType.fields.toList
    assertTrue(actualFields == expectedFields,
      s"Expected StructType with fields 'a' (IntegerType), 'b' (DoubleType), 'c' (ArrayType" +
        s"(IntegerType)) but got $actualFields")
  }


  @Test
  def testFromValueWithArrayTypes(): Unit = {
    val arrayValue = Seq(1, 2, 2.4, 6, null)
    val arrayResult = EnrichedDataType.fromValue(arrayValue)

    assertTrue(arrayResult.isInstanceOf[EnrichedArrayType],
      "Expected EnrichedArrayType for array input.")
  }


  @Test
  def testFromValueWithMapTypes(): Unit = {
    val mapValue = Map(1 -> 2, 2 -> 3, "a" -> 5)
    val mapResult = EnrichedDataType.fromValue(mapValue)

    assertTrue(mapResult.isInstanceOf[EnrichedMapType],
      "Expected EnrichedMapType for map input.")
  }


  @Test
  def testBasicDataTypes(): Unit = {
    assertEquals(EnrichedBooleanType, fromSparkDataType(BooleanType))
    assertEquals(EnrichedIntegerType, fromSparkDataType(IntegerType))
    assertEquals(EnrichedLongType, fromSparkDataType(LongType))
    assertEquals(EnrichedDoubleType, fromSparkDataType(DoubleType))
    assertEquals(EnrichedStringType, fromSparkDataType(StringType))
  }

  @Test
  def testDecimalType(): Unit = {
    val decimalType = DecimalType(10, 2)
    assertEquals(EnrichedDecimalType(10, 2), fromSparkDataType(decimalType))
  }

  @Test
  def testArrayType(): Unit = {
    val arrayType = ArrayType(IntegerType)
    assertEquals(EnrichedArrayType(EnrichedIntegerType), fromSparkDataType(arrayType))
  }

  @Test
  def testMapType(): Unit = {
    val mapType = MapType(StringType, IntegerType)
    assertEquals(EnrichedMapType(EnrichedStringType, EnrichedIntegerType), fromSparkDataType
    (mapType))
  }

  @Test
  def testStructType(): Unit = {
    val structType = StructType(Seq(
      StructField("field1", StringType),
      StructField("field2", IntegerType)
    ))
    val expectedEnrichedStructType = EnrichedStructType(ListMap(
      "field1" -> EnrichedStringType,
      "field2" -> EnrichedIntegerType
    ))

    assertEquals(expectedEnrichedStructType, fromSparkDataType(structType))
  }

  @Test
  def testUnsupportedDataType(): Unit = {
    val unsupportedDataType = BinaryType

    assertThrows(classOf[UnsupportedDataTypeException], () => fromSparkDataType
    (unsupportedDataType))
  }
}


