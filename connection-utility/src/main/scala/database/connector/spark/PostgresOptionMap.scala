package database.connector.spark

import database.model.DatabaseConfig

object PostgresOptionMap extends DatabaseOptionMap {
  override def getSparkConnectionMap(databaseConfig: DatabaseConfig): Map[String, String] = {
    val url = s"${databaseConfig.databaseUrl.get}?user=${databaseConfig.username.get}&password=${databaseConfig.password.get}"

    val jdbcOptions = Map(
      "url" -> url,
      "sessionInitStatement" -> s"set statement_timeout to 3600000;"
    )
    jdbcOptions

  }

}
