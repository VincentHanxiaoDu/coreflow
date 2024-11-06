package dev.coreflow.spark.etl.batch

import dev.coreflow.spark.etl.params.Params
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => F}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.ArgumentMatchers.{eq => eqTo}
import org.mockito.Mockito._
import org.mockito.{Mock, MockitoAnnotations}

class ParametrizedWritableTableTest {

  @Mock
  private var sparkSession: SparkSession = _

  @Mock
  private var sourceDataFrame: DataFrame = _

  @Mock
  private var writeDataFrame: DataFrame = _

  @Mock
  private var params: PartitionParams = _

  private trait PartitionParams extends Params {
    def sourceFilter: Column
  }

  private case class ConcreteParametrizedWritableTable(tableDB: String, tableName: String)
    extends dev.coreflow.spark.etl.batch.ParametrizedWritableTable[PartitionParams] {

    override def writeFull(params: PartitionParams)(implicit spark: SparkSession): Unit = {
      spark.table(s"source_db.source_table").filter(params.sourceFilter)
    }
  }

  @BeforeEach
  def setUp(): Unit = {
    MockitoAnnotations.openMocks(this)
    when(sparkSession.table(eqTo("source_db.source_table"))).thenReturn(sourceDataFrame)
    when(params.sourceFilter).thenReturn(F.col("column1") === "value1")
  }

  @Test
  def validWriteTableTest(): Unit = {
    val table = ConcreteParametrizedWritableTable("table_db", "table_name")
    table.writeFull(params)(sparkSession)
    verify(sparkSession).table("source_db.source_table")
    verify(sourceDataFrame).filter(F.col("column1") === "value1")
  }

  @Test
  def implicitSparkSessionTest(): Unit = {
    implicit val impSpark: SparkSession = sparkSession
    val table = ConcreteParametrizedWritableTable("valid_db_name", "valid_table_name")
    table.writeFull(params)
  }

  @Test
  def invalidDatabaseNameTest(): Unit = {
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => ConcreteParametrizedWritableTable("1invalid_db", "valid_table")
    )
    assertTrue(exception.getMessage.contains("Invalid database name"))
  }

  @Test
  def invalidTableNameTest(): Unit = {
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => ConcreteParametrizedWritableTable("valid_db", "")
    )
    assertEquals("requirement failed: tableName cannot be empty", exception.getMessage)
  }
}