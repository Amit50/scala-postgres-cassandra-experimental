package database.operations

import org.apache.spark.sql.DataFrame

abstract class SparkDBOperationTrait {

  def getTableData(tableName: String): DataFrame

  def getTablesList(allowedTableSchema: Seq[String], tableType: String): DataFrame


}
