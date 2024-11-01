package dev.coreflow.spark.etl.batch.params

import org.apache.spark.sql.Column
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.mockito.Mockito._

class ReadPartitionParamsTest {

  @Mock
  private val filter: Column = mock(classOf[Column])

  private class ConcreteReadPartitionParams extends
    dev.coreflow.spark.etl.batch.params.ReadPartitionParams {
    override def partitionFilter: Column = filter
  }

  private object ConcreteReadPartitionParams {
    def apply(): ConcreteReadPartitionParams = new ConcreteReadPartitionParams
  }

  @Test
  def validPartitionFilterTest(): Unit = {
    val params = ConcreteReadPartitionParams()
    val result = params.partitionFilter
    assertEquals(filter, result, "The returned Column should match the mocked one")
  }
}
