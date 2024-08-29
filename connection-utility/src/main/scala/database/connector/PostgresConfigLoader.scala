package database.connector

import database.model.DatabaseConfig

import java.io.FileInputStream
import java.util.Properties

object PostgresConfigLoader extends DatabaseConfigLoader {
  override def getDatabaseConfiguration(filePath: String = "database.properties"): DatabaseConfig = {
    val props = new Properties()
    try {
      props.load(new FileInputStream(filePath))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    DatabaseConfig(
      databaseUrl = Some(props.getProperty("db.postgres.url")),
      username = Some(props.getProperty("db.postgres.username")),
      password = Some(props.getProperty("db.postgres.password")),
      driver = Some(props.getProperty("db.postgres.driver"))
    )
  }
}
