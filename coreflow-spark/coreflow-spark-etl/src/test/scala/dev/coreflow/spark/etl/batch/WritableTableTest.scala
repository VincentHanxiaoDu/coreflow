package dev.coreflow.spark.etl.batch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.jupiter.api.Assertions.{assertThrows, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.ArgumentMatchers.{eq => eqTo}
import org.mockito.Mockito._
import org.mockito.{Mock, MockitoAnnotations}


class WritableTableTest {

  @Mock
  private var sparkSession: SparkSession = _

  @Mock
  private var dataFrame: DataFrame = _

  private case class ConcreteWritableTable(tableDB: String, tableName: String)
    extends dev.coreflow.spark.etl.batch.WritableTable {

    override def writeFull(implicit spark: SparkSession): Unit = {
      spark.table(s"source_db.source_table")
    }
  }

  @BeforeEach
  def setUp(): Unit = {
    MockitoAnnotations.openMocks(this)
    when(sparkSession.table(eqTo("source_db.source_table"))).thenReturn(dataFrame)
  }


  @Test
  def validWriteTableTest(): Unit = {
    val writableTable = ConcreteWritableTable("table_db", "table_name")
    writableTable.writeFull(sparkSession)
    verify(sparkSession).table("source_db.source_table")
  }

  @Test
  def implicitSparkSessionTest(): Unit = {
    val writableTable = ConcreteWritableTable("table_db", "table_name")
    implicit val impSpark: SparkSession = sparkSession
    writableTable.writeFull
  }

  @Test
  def invalidTableDBTest(): Unit = {
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => ConcreteWritableTable("123invalid", "valid_table")
    )
    assertTrue(exception.getMessage.contains("Invalid database name"))
  }

  @Test
  def invalidTableNameTest(): Unit = {
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => ConcreteWritableTable("valid_db", "123invalid")
    )
    assertTrue(exception.getMessage.contains("Invalid table name"))
  }
}
