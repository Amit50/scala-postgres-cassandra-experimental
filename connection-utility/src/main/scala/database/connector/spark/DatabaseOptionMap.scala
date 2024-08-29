package database.connector.spark

import database.model.DatabaseConfig

trait DatabaseOptionMap {
  def getSparkConnectionMap(databaseConfig: DatabaseConfig): Map[String, String]
}
