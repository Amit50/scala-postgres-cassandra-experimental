package database.operations

import database.connector.PostgresConfigLoader
import database.connector.spark.PostgresOptionMap
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

class SparkPostgresOperation(
                              propertiesFilePath: String = "database.properties"
                            )(implicit spark: SparkSession) extends SparkDBOperationTrait {

  private val connectionOptions: Map[String, String] = PostgresOptionMap.getSparkConnectionMap(PostgresConfigLoader.getDatabaseConfiguration(propertiesFilePath))

  private val dataFrameReader: DataFrameReader = spark.read.format("jdbc").options(connectionOptions)

  override def getTableData(tableName: String): DataFrame = {
    dataFrameReader.option("dbtable", tableName).load()
  }

  override def getTablesList(allowedTableSchemas: Seq[String], tableType: String = "BASE TABLE"): DataFrame = {
    getTableData("information_schema.tables").filter(col("table_type") === tableType).filter(col("table_schema").isin(allowedTableSchemas: _*))
  }

  def writeDataToTable(inputDF: DataFrame, tableName: String, mode: String = "overwrite"): Unit = {
    inputDF.write.format("jdbc").options(connectionOptions).mode(mode).option("dbtable", tableName).save()
  }
}
