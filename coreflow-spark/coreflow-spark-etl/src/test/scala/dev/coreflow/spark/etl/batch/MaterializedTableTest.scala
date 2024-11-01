package dev.coreflow.spark.etl.batch

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test


class MaterializedTableTest {
  private class ConcreteMaterializedTable(override val tableDB: String, override val
  tableName: String)
    extends dev.coreflow.spark.etl.batch.MaterializedTable {

  }

  private object ConcreteMaterializedTable {
    def apply(tableDB: String, tableName: String): ConcreteMaterializedTable = new
        ConcreteMaterializedTable(tableDB, tableName)
  }

  @Test
  def validDatabaseNameTest(): Unit = {
    val table = ConcreteMaterializedTable("valid_db_name", "valid_table_name")
    assertNotNull(table)
    assertEquals("valid_db_name", table.tableDB)
  }

  @Test
  def nullDatabaseNameTest(): Unit = {
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => ConcreteMaterializedTable(null, "valid_table_name")
    )
    assertEquals("requirement failed: tableDB cannot be null", exception.getMessage)
  }

  @Test
  def emptyDatabaseNameTest(): Unit = {
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => ConcreteMaterializedTable("", "valid_table_name")
    )
    assertEquals("requirement failed: tableDB cannot be empty", exception.getMessage)
  }

  @Test
  def invalidDatabaseNameWithSpecialCharsTest(): Unit = {
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => ConcreteMaterializedTable("invalid-name!", "valid_table_name")
    )
    assertTrue(exception.getMessage.contains("Invalid database name"))
  }

  @Test
  def databaseNameStartingWithNumberTest(): Unit = {
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => ConcreteMaterializedTable("1invalid", "valid_table_name")
    )
    assertTrue(exception.getMessage.contains("Invalid database name"))
  }

  @Test
  def singleCharacterDatabaseNameTest(): Unit = {
    val table = ConcreteMaterializedTable("a", "valid_table_name")
    assertNotNull(table)
    assertEquals("a", table.tableDB)
  }

  @Test
  def databaseNameWithNumbersAndUnderscoresTest(): Unit = {
    val table = ConcreteMaterializedTable("db_123_name", "valid_table_name")
    assertNotNull(table)
    assertEquals("db_123_name", table.tableDB)
  }

  @Test
  def invalidTableNameTest(): Unit = {
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => ConcreteMaterializedTable("valid_db_name", "123invalid_table_name")
    )
    assertTrue(exception.getMessage.contains("Invalid table name"))
  }
}
