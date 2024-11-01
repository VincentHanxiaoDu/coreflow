package dev.coreflow.spark.util.schema.merge

import dev.coreflow.spark.util.schema.datatype.impl.EnrichedStructType

trait SchemaMerger {
  def getUpdateSQLStatements
  (
    currentSchema: EnrichedStructType,
    newSchema: EnrichedStructType,
    tableIdentifier: String,
    tempTableIdentifier: String,
    partitionColumns: Seq[String]
  ): Seq[String]
}
