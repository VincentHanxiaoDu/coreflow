package dev.coreflow.spark.util.schema.merge.impl

import dev.coreflow.spark.util.schema.datatype.impl._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.immutable.ListMap

class HiveTableSchemaMergerTest {

  val tableIdentifier = "test_table"
  val tempTableIdentifier = "temp_test_table"
  val partitionColumns: Seq[String] = Seq("partition_col")

  val currentSchema: EnrichedStructType = EnrichedStructType(
    ListMap(
      "id" -> EnrichedIntegerType,
      "name" -> EnrichedStringType,
      "age" -> EnrichedIntegerType
    )
  )

  val newSchemaWithNewColumns: EnrichedStructType = EnrichedStructType(
    ListMap(
      "id" -> EnrichedIntegerType,
      "name" -> EnrichedStringType,
      "new_column" -> EnrichedStringType
    )
  )

  val newSchemaWithUpdatedColumns: EnrichedStructType = EnrichedStructType(
    ListMap(
      "id" -> EnrichedIntegerType,
      "name" -> EnrichedStringType,
      "age" -> EnrichedDoubleType
    )
  )

  val schemaMerger = new HiveTableSchemaMerger

  @Test
  def testGetAddColumnsDDLStatementsWithNewColumns(): Unit = {
    val addColumnsDDL = schemaMerger.getAddColumnsDDLStatements(currentSchema,
      newSchemaWithNewColumns, tableIdentifier)

    assertEquals(
      Seq("ALTER TABLE test_table ADD COLUMNS (`new_column` STRING);"),
      addColumnsDDL,
      "Expected ALTER TABLE statement to add new columns in the schema"
    )
  }

  @Test
  def testGetAddColumnsDDLStatementsWithoutNewColumns(): Unit = {
    val addColumnsDDL = schemaMerger.getAddColumnsDDLStatements(currentSchema, currentSchema,
      tableIdentifier)
    assertTrue(addColumnsDDL.isEmpty, "Expected no ADD COLUMNS statement when no new columns exist")
  }

  @Test
  def testGetUpdateDDLStatementsWithChangedFields(): Unit = {
    val updateDDL = schemaMerger.getUpdateDDLStatements(currentSchema,
      newSchemaWithUpdatedColumns, tableIdentifier, tempTableIdentifier, partitionColumns)

    assertEquals(
      Seq(
        "DROP TABLE IF EXISTS temp_test_table;",
        "CREATE TABLE temp_test_table PARTITIONED BY (partition_col) AS SELECT `id`, `name`, CAST" +
          "(`age` AS DOUBLE) AS `age` FROM test_table",
        "DROP TABLE IF EXISTS test_table;",
        "ALTER TABLE temp_test_table RENAME TO test_table;",
        "REFRESH TABLE test_table;"
      ),
      updateDDL,
      "Expected update DDL statements to handle changed fields by recreating table with updated " +
        "schema"
    )
  }

  @Test
  def testGetUpdateDDLStatementsWithoutChangedFields(): Unit = {
    val updateDDL = schemaMerger.getUpdateDDLStatements(currentSchema, currentSchema,
      tableIdentifier, tempTableIdentifier, partitionColumns)
    assertTrue(updateDDL.isEmpty, "Expected no update DDL statements when no fields have changed")
  }


  @Test
  def testGetUpdateSQLStatementsWithoutNewColumnsOrChanges(): Unit = {
    val updateSQLStatements = schemaMerger.getUpdateSQLStatements(currentSchema, currentSchema,
      tableIdentifier, tempTableIdentifier, partitionColumns)
    assertTrue(updateSQLStatements.isEmpty, "Expected no SQL statements when there are no new " +
      "columns or schema changes")
  }
}

