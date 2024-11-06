package dev.coreflow.spark.etl.batch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.ArgumentMatchers.{eq => eqTo}
import org.mockito.Mockito._
import org.mockito.{Mock, MockitoAnnotations}

class ReadableTableTest {

  @Mock
  private var sparkSession: SparkSession = _

  @Mock
  private var dataFrame: DataFrame = _

  private case class ConcreteReadableTable(tableName: String)
    extends ReadableTable {

    override def readTable(implicit spark: SparkSession): DataFrame = {
      spark.table(s"$tableName")
    }
  }


  @BeforeEach
  def setUp(): Unit = {
    MockitoAnnotations.openMocks(this)
  }

  @Test
  def validReadTableTest(): Unit = {
    when(sparkSession.table(eqTo("table_name"))).thenReturn(dataFrame)
    val readableTale = ConcreteReadableTable("table_name")
    val result = readableTale.readTable(sparkSession)
    assertEquals(dataFrame, result, "The returned DataFrame should match the mocked one")
  }

  @Test
  def implicitSparkSessionTest(): Unit = {
    when(sparkSession.table(eqTo("table_name"))).thenReturn(dataFrame)
    val readableTale = ConcreteReadableTable("table_name")
    implicit val impSpark: SparkSession = sparkSession
    val result = readableTale.readTable
    assertEquals(dataFrame, result, "The returned DataFrame should match the mocked one")
  }

  @Test
  def invalidTableNameTest(): Unit = {
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => ConcreteReadableTable("123invalid")
    )
    assertTrue(exception.getMessage.contains("Invalid table name"))
  }
}
