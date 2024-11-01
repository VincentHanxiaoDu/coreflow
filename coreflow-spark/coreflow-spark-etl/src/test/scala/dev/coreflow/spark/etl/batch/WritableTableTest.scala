package dev.coreflow.spark.etl.batch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.mockito.Mockito._

class WritableTableTest {

  @Mock
  private val spark: SparkSession = mock(classOf[SparkSession])

  @Mock
  private val dataFrame: DataFrame = mock(classOf[DataFrame])

  private class ConcreteWritableTable extends dev.coreflow.spark.etl.batch.WritableTable {
    override def writeFull()(implicit spark: SparkSession): Unit = {}

    override def tableName: String = "table_name"

    override def tableDB: String = "table_db"
  }

  private object ConcreteWritableTable {
    def apply(): ConcreteWritableTable = new ConcreteWritableTable
  }

  @Test
  def validWriteTableTest(): Unit = {
    val table = ConcreteWritableTable()
    table.writeFull()(spark)
  }

  @Test
  def implicitSparkSessionTest(): Unit = {
    val table = ConcreteWritableTable()
    implicit val impSpark: SparkSession = spark
    table.writeFull()
  }
}
