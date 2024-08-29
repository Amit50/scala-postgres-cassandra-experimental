package dbwriter

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object PgWriter {

  val pgOptionsMap: Map[String, String] = Map(
    "user" -> "postgres",
    "password" -> "postgres",
    "driver" -> "org.postgresql.Driver"
  )

  def writePgTable(
                    df: DataFrame,
                    tableName: String,
                    dbName: String = "public",
                    jdbcUrl: String = "jdbc:postgresql://localhost:5432/postgres"
                  )(implicit spark: SparkSession): Unit = {
    df.write.format("jdbc").options(pgOptionsMap).option("dbTable", s"$dbName.$tableName").option("url", jdbcUrl).mode(SaveMode.Overwrite).save()
  }

}
