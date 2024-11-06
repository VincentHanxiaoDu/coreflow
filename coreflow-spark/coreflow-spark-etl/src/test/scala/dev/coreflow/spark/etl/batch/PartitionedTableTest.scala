package dev.coreflow.spark.etl.batch

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.immutable.HashSet

class PartitionedTableTest {

  @Test
  def testIsPartitionSubset(): Unit = {
    case object ConcretePartitionedTable extends PartitionedTable {
      override def partitionColumns: HashSet[String] =
        HashSet("partition_column1", "partition_column2")

      override def tableName: String = "test_table_name"
    }

    assertTrue(ConcretePartitionedTable.isPartitionSubset(
      Set("partition_column1", "partition_column2")))
    assertTrue(ConcretePartitionedTable.isPartitionSubset(Set("partition_column1")))
    assertTrue(ConcretePartitionedTable.isPartitionSubset(Set("partition_column2")))
    assertFalse(ConcretePartitionedTable.isPartitionSubset(
      Set("partition_column1", "partition_column2", "column3")))
    assertFalse(ConcretePartitionedTable.isPartitionSubset(
      Set("column3")))
  }

  @Test
  def testEmptyPartitionColumns(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () => new PartitionedTable {
        override def partitionColumns: HashSet[String] = HashSet.empty

        override def tableName: String = "test_table_name"
      }
    )
  }
}
