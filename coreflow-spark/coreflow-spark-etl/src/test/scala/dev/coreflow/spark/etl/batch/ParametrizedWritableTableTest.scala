package dev.coreflow.spark.etl.batch

import dev.coreflow.spark.etl.params.Params
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.mockito.Mockito._

class ParametrizedWritableTableTest {

  @Mock
  private val spark: SparkSession = mock(classOf[SparkSession])

  @Mock
  private val params: Params = mock(classOf[Params])

  private class ConcreteParametrizedWritableTable
  (
    override val tableDB: String,
    override val tableName: String
  ) extends dev.coreflow.spark.etl.batch.ParametrizedWritableTable[Params] {

    override def writeFull(params: Params)(implicit spark: SparkSession): Unit = {
    }
  }

  private object ConcreteParametrizedWritableTable {
    def apply(tableDB: String, tableName: String): ConcreteParametrizedWritableTable =
      new ConcreteParametrizedWritableTable(tableDB, tableName)
  }

  @Test
  def implicitSparkSessionTest(): Unit = {
    val table = ConcreteParametrizedWritableTable("valid_db", "valid_table")
    implicit val impSpark: SparkSession = spark
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