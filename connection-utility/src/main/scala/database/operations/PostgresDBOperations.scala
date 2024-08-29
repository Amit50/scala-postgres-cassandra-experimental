package database.operations

import database.model.DatabaseConfig

import java.sql.Connection

object PostgresDBOperations extends DatabaseOperations {

  def getConnection(dbConfig: DatabaseConfig): Connection = java.sql.DriverManager.getConnection(dbConfig.databaseUrl.get, dbConfig.username.get, dbConfig.password.get)

  def createIndex(sqlStatement: String, dbConfig: DatabaseConfig): Unit = {
    val connection = getConnection(dbConfig)
    try {
      val statement = connection.createStatement()
      statement.execute(sqlStatement)
      statement.close()
    } finally {
      connection.close()
    }
  }

  def createIndex(tableName: String,
                  indexName: String,
                  indexColumns: Seq[String],
                  additionalIncludeColumns: Seq[String] = Seq.empty,
                  dbConfig: DatabaseConfig): Unit = {
    val connection = getConnection(dbConfig)
    val additionalColumns: String = additionalIncludeColumns.nonEmpty match {
      case true => s" INCLUDE(${additionalIncludeColumns.mkString(",")})"
      case false => ""
    }
    val sqlStatement = s"CREATE INDEX $indexName ON $tableName(${indexColumns.mkString(",")}) ${additionalColumns};"
    try {
      val statement = connection.createStatement()
      statement.execute(sqlStatement)
      println(sqlStatement)
      statement.close()
    } finally {
      connection.close()
    }
  }

  def dropIndex(indexName: String,
                dbConfig: DatabaseConfig): Unit = {
    val connection = getConnection(dbConfig)
    val sqlStatement = s"DROP INDEX $indexName;"

    try {
      val statement = connection.createStatement()
      statement.execute(sqlStatement)
      println(sqlStatement)
      statement.close()
    } finally {
      connection.close()
    }
  }


}
