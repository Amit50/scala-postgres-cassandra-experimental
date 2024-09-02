package database.operations

import database.model.DatabaseConfig

import java.sql.{Connection, Date, PreparedStatement, Timestamp}

class PostgresDBOperations(dbConfig: DatabaseConfig) extends DatabaseOperations {

  def getConnection: Connection = java.sql.DriverManager.getConnection(dbConfig.databaseUrl.get, dbConfig.username.get, dbConfig.password.get)

  def createIndex(sqlStatement: String): Unit = {
    val connection = getConnection
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
                  additionalIncludeColumns: Seq[String] = Seq.empty
                 ): Unit = {
    val connection = getConnection
    val additionalColumns: String = additionalIncludeColumns.nonEmpty match {
      case true => s" INCLUDE(${additionalIncludeColumns.mkString(",")})"
      case false => ""
    }
    val sqlStatement = s"CREATE INDEX IF NOT EXISTS $indexName ON $tableName(${indexColumns.mkString(",")}) ${additionalColumns};"
    try {
      val statement = connection.createStatement()
      statement.execute(sqlStatement)
      println(sqlStatement)
      statement.close()
    } finally {
      connection.close()
    }
  }

  def dropIndex(indexName: String): Unit = {
    val connection = getConnection
    val sqlStatement = s"DROP INDEX IF EXISTS $indexName;"

    try {
      val statement = connection.createStatement()
      statement.execute(sqlStatement)
      println(sqlStatement)
      statement.close()
    } finally {
      connection.close()
    }
  }

  // Update function
  def updatePostgresTable(tableName: String, name: String, age: Int, salary: Int): Unit = {
    val connection: Connection = getConnection

    try {
      // Establish connection
      connection.setAutoCommit(false)
      // Prepare and execute the update statement
      var updateStatement = connection.prepareStatement(
        s"UPDATE $tableName SET salary = $salary WHERE name = '$name'"
      )
      updateStatement.executeUpdate()
      updateStatement = connection.prepareStatement(
        s"UPDATE $tableName SET age = $age WHERE name = '$name'"
      )
      updateStatement.executeUpdate()
      println(s"statement exuected on $tableName for $name and $age and $salary");
      // Commit the transaction
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  def upsertBatch(tableName: String, batch: Iterator[(Long, String, Int, Int, Date,Timestamp,Timestamp)]): Unit = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    try {
      connection = getConnection
      connection.setAutoCommit(false)
      val upsertSQL =
        s"""
            INSERT INTO $tableName (id,name,age,salary,salary_date,created_at,updated_at)
            VALUES (?, ?, ?, ?, ? , ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                updated_at= EXCLUDED.updated_at
                ;
        """
      preparedStatement = connection.prepareStatement(upsertSQL)
      batch.foreach({
        case (id, name, age, salary, salaryDate,createdAt,updatedAt) =>
          preparedStatement.setLong(1, id)
          preparedStatement.setString(2, name)
          preparedStatement.setInt(3, age)
          preparedStatement.setInt(4, salary)
          preparedStatement.setDate(5, salaryDate)
          preparedStatement.setTimestamp(6, createdAt)
          preparedStatement.setTimestamp(7, updatedAt)

          preparedStatement.addBatch()
      })
      preparedStatement.executeBatch()
      connection.commit()
      println(s"batch commit exuected on $tableName");
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
    }

  }


}
