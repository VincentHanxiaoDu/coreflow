package dev.coreflow.spark.etl.batch

import dev.coreflow.spark.etl.batch.params.ReadPartitionParams
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.mockito.Mockito._

import scala.collection.immutable.HashSet

class PartitionedTableTest {

  @Mock
  private val spark: SparkSession = mock(classOf[SparkSession])

  @Mock
  private val dataFrame: DataFrame = mock(classOf[DataFrame])

  @Mock
  private val partitionedDataFrame: DataFrame = mock(classOf[DataFrame])

  @Mock
  private val readParams: ReadPartitionParams = mock(classOf[ReadPartitionParams])

  private class ConcretePartitionedTable extends PartitionedTable[ReadPartitionParams] {

    override def readTable()(implicit spark: SparkSession): DataFrame = {
      spark.table(s"$tableName")
      dataFrame
    }

    override def tableName: String = "table_name"

    override def partitionColumns: HashSet[String] =
      HashSet("partition_column1", "partition_column2")
  }

  private object ConcretePartitionedTable {
    def apply(): ConcretePartitionedTable = new ConcretePartitionedTable
  }

  @Test
  def testIsPartitionSubset(): Unit = {
    val partitionedTable = ConcretePartitionedTable()

    assertTrue(partitionedTable.isPartitionSubset(
      Set("partition_column1", "partition_column2")))
    assertTrue(partitionedTable.isPartitionSubset(Set("partition_column1")))
    assertTrue(partitionedTable.isPartitionSubset(Set("partition_column2")))
    assertFalse(partitionedTable.isPartitionSubset(
      Set("partition_column1", "partition_column2", "column3")))
    assertFalse(partitionedTable.isPartitionSubset(
      Set("column3")))
  }
}
