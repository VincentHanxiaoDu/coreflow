package dev.coreflow.spark.util.schema.generator.impl

import dev.coreflow.spark.util.schema.datatype.EnrichedDataType
import dev.coreflow.spark.util.schema.datatype.impl.{EnrichedIntegerType, EnrichedStringType, EnrichedStructType}
import org.apache.spark.rdd.RDD
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito._

import scala.collection.immutable.ListMap

class MapSchemaGeneratorTest {

  @Test
  def testGetInferredSchema(): Unit = {

    val rdd = mock(classOf[RDD[Map[String, _]]])
    val rddDataType = mock(classOf[RDD[EnrichedDataType]])
    val resultSchema = EnrichedStructType(
      ListMap("name" -> EnrichedStringType, "age" -> EnrichedIntegerType))
    when(rdd.map(EnrichedDataType.fromValue)).thenReturn(rddDataType)
    when(rddDataType.reduce(_ reduceToCompatibleDataType _)).thenReturn(
      EnrichedStructType(ListMap("name" -> EnrichedStringType, "age" -> EnrichedIntegerType)))

    assertEquals(EnrichedStructType(ListMap("name" -> EnrichedStringType, "age" ->
      EnrichedIntegerType)), resultSchema)
  }
}
