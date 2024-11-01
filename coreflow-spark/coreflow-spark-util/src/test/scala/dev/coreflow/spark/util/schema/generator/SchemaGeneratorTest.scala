package dev.coreflow.spark.util.schema.generator

import dev.coreflow.spark.util.schema.datatype.impl.EnrichedStructType
import org.apache.spark.rdd.RDD
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{eq => eqTo}
import org.mockito.Mockito._

class SchemaGeneratorTest {

  val enrichedStructType: EnrichedStructType = mock(classOf[EnrichedStructType])

  case class TestSchemaGenerator() extends SchemaGenerator {
    override def getInferredSchema(rdd: RDD[Map[String, _]]): EnrichedStructType =
      enrichedStructType
  }

  @Test
  def testGetInferredSchemaWithSampling(): Unit = {
    val mockRdd = mock(classOf[RDD[Map[String, _]]])
    val mockSampledRdd = mock(classOf[RDD[Map[String, _]]])

    val schemaGenerator = TestSchemaGenerator()

    when(mockRdd.sample(eqTo(false), eqTo(0.5), eqTo(13L))).thenReturn(mockSampledRdd)
    val result = schemaGenerator.getInferredSchema(mockRdd, 0.5, 13L)
    verify(mockRdd).sample(withReplacement = false, 0.5, 13L)
    assertNotNull(result, "Expected non-null result from getInferredSchema")
  }

  @Test
  def testGetInferredSchemaWithInvalidSamplingRatio(): Unit = {
    val mockRdd = mock(classOf[RDD[Map[String, _]]])
    val schemaGenerator = TestSchemaGenerator()

    val exception1 = assertThrows(classOf[IllegalArgumentException], () => {
      schemaGenerator.getInferredSchema(mockRdd, -0.5)
    })
    assertTrue(exception1.getMessage.contains("Sampling ratio must be in (0, 1)."))

    val exception2 = assertThrows(classOf[IllegalArgumentException], () => {
      schemaGenerator.getInferredSchema(mockRdd, 1.5)
    })
    assertTrue(exception2.getMessage.contains("Sampling ratio must be in (0, 1)."))
  }
}

