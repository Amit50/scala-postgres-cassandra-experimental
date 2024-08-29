package database.connector

import database.model.DatabaseConfig

trait DatabaseConfigLoader {


  def getDatabaseConfiguration(filePath: String): DatabaseConfig

}
