package database.model

case class DatabaseConfig(
                           databaseUrl: Option[String],
                           username: Option[String],
                           password: Option[String],
                           driver: Option[String]
                         )

