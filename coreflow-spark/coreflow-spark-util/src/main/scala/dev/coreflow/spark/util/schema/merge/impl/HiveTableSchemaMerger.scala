package dev.coreflow.spark.util.schema.merge.impl

import dev.coreflow.spark.util.schema.datatype.impl.EnrichedStructType
import dev.coreflow.spark.util.schema.merge.SchemaMerger

case object HiveTableSchemaMerger extends SchemaMerger {
  override def getUpdateSQLStatements
  (
    currentSchema: EnrichedStructType,
    newSchema: EnrichedStructType,
    tableIdentifier: String,
    tempTableIdentifier: String,
    partitionColumns: Seq[String]
  ): Seq[String] = {
    val addColumnsDDLStatements = getAddColumnsDDLStatements(currentSchema, newSchema,
      tableIdentifier)
    val updateDDLStatements = getUpdateDDLStatements(currentSchema, newSchema, tableIdentifier,
      tempTableIdentifier, partitionColumns)
    addColumnsDDLStatements ++ updateDDLStatements
  }

  def getAddColumnsDDLStatements
  (
    currentSchema: EnrichedStructType,
    newSchema: EnrichedStructType,
    tableIdentifier: String
  ): Seq[String] = {
    val addColumnFields = newSchema.internalDataTypes.filterKeys(
      k => !currentSchema.internalDataTypes.contains(k)).map {
      case (k, v) => s"`$k` ${v.toSparkDataType.sql}"
    }
    val addColumnsStatement = if (addColumnFields.nonEmpty) {
      Seq(s"ALTER TABLE $tableIdentifier ADD COLUMNS (${addColumnFields.mkString(", ")});")
    } else {
      Seq.empty
    }
    addColumnsStatement
  }


  def getUpdateDDLStatements
  (
    currentSchema: EnrichedStructType,
    newSchema: EnrichedStructType,
    tableIdentifier: String,
    tempTableIdentifier: String,
    partitionColumns: Seq[String]
  ): Seq[String] = {
    val (hasChangedFields, insertOverwriteFields) = currentSchema.internalDataTypes
      .foldLeft(false: Boolean, Seq.empty[String]) {
        case ((changed, fields), (k, o)) =>
          newSchema.internalDataTypes.get(k).map {
            case n if o.reduceToCompatibleDataType(n) == o =>
              (changed, fields :+ s"`$k`")
            case n =>
              (true, fields :+ s"CAST(`$k` AS ${n.toSparkDataType.sql}) AS `$k`")
          }.getOrElse(false, fields :+ s"`$k`")
      }

    val refreshStatements = if (hasChangedFields) {
      Seq(
        s"DROP TABLE IF EXISTS $tempTableIdentifier;",
        s"CREATE TABLE $tempTableIdentifier ${
          if (partitionColumns.nonEmpty) s"PARTITIONED BY " +
            s"(${partitionColumns.mkString(", ")})" else ""
        } AS SELECT ${
          insertOverwriteFields
            .mkString(", ")
        } FROM $tableIdentifier",
        s"DROP TABLE IF EXISTS $tableIdentifier;",
        s"ALTER TABLE $tempTableIdentifier RENAME TO $tableIdentifier;",
        s"REFRESH TABLE $tableIdentifier;"
      )
    } else {
      Seq.empty
    }
    refreshStatements
  }
}
