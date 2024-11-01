package dev.coreflow.spark.etl.batch

import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => F}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{eq => eqTo}
import org.mockito.Mock
import org.mockito.Mockito._

import scala.collection.immutable.HashSet

class PartitionedTableTest {

  @Mock
  private val spark: SparkSession = mock(classOf[SparkSession])

  @Mock
  private val dataFrame: DataFrame = mock(classOf[DataFrame])

  @Mock
  private val dataFramePartitions: DataFrame = mock(classOf[DataFrame])

  @Mock
  private val column: Column = mock(classOf[Column])

  private class ConcretePartitionedTable extends PartitionedTable {

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


  @Test
  def testReadPartitions(): Unit = {
    val partitionedTable = ConcretePartitionedTable()

    when(dataFrame.filter(eqTo(F.col("partition_column1") === 1))).thenReturn(dataFramePartitions)
    when(spark.table("table_name")).thenReturn(dataFrame)

    val result = partitionedTable.readPartitions(F.col("partition_column1") === 1)(spark)

    verify(dataFrame).filter(eqTo(F.col("partition_column1") === 1))
    assertEquals(dataFramePartitions, result)
  }
}
