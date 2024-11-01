package dev.coreflow.spark.etl.batch

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test


class LogicalTableTest {

  private class ConcreteLogicalTable(override val tableName: String) extends
    dev.coreflow.spark.etl.batch.LogicalTable

  private object ConcreteLogicalTable {
    def apply(tableName: String): ConcreteLogicalTable = new ConcreteLogicalTable(tableName)
  }

  @Test
  def validTableNameTest(): Unit = {
    val table = ConcreteLogicalTable("valid_table_name")
    assertNotNull(table)
    assertEquals("valid_table_name", table.tableName)
  }

  @Test
  def invalidTableNameTest(): Unit = {
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => ConcreteLogicalTable("123invalid")
    )
    assertTrue(exception.getMessage.contains("Invalid table name"))
  }

  @Test
  def nullTableNameTest(): Unit = {
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => ConcreteLogicalTable(null)
    )
    assertEquals("requirement failed: tableName cannot be null", exception.getMessage)
  }

  @Test
  def emptyTableNameTest(): Unit = {
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => ConcreteLogicalTable("")
    )
    assertEquals("requirement failed: tableName cannot be empty", exception.getMessage)
  }

  @Test
  def tableNameWithSpecialCharsTest(): Unit = {
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => ConcreteLogicalTable("invalid-name!")
    )
    assertTrue(exception.getMessage.contains("Invalid table name"))
  }

  @Test
  def singleCharacterTableNameTest(): Unit = {
    val table = ConcreteLogicalTable("a")
    assertNotNull(table)
    assertEquals("a", table.tableName)
  }

  @Test
  def singleNumberTableNameTest(): Unit = {
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => ConcreteLogicalTable("1")
    )
    assertTrue(exception.getMessage.contains("Invalid table name"))
  }
}
