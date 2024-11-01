package dev.coreflow.spark.etl.batch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.mockito.Mockito._

class ReadableTableTest {

  @Mock
  private val spark: SparkSession = mock(classOf[SparkSession])

  @Mock
  private val dataFrame: DataFrame = mock(classOf[DataFrame])

  private class ConcreteReadableTable extends dev.coreflow.spark.etl.batch.ReadableTable {
    override def readTable()(implicit spark: SparkSession)
    : DataFrame = {
      dataFrame
    }

    override def tableName: String = "table_name"
  }

  private object ConcreteReadableTable {
    def apply(): ConcreteReadableTable = new ConcreteReadableTable
  }

  @Test
  def validReadTableTest(): Unit = {
    val table = ConcreteReadableTable()
    val result = table.readTable()(spark)
    assertEquals(dataFrame, result, "The returned DataFrame should match the mocked one")
  }

  @Test
  def implicitSparkSessionTest(): Unit = {
    val table = ConcreteReadableTable()
    implicit val impSpark: SparkSession = spark
    val result = table.readTable()
    assertEquals(dataFrame, result, "The returned DataFrame should match the mocked one")
  }
}
