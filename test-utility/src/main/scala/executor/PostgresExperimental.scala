package executor

import database.connector.PostgresConfigLoader
import database.operations.{PostgresDBOperations, SparkPostgresOperation}
import helper.data.generator.SampleDataGenerator.getSampleEmployeeData
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.{Date, Timestamp}

object PostgresExperimental {

  def updateRecordToPg(df: DataFrame, pgDBOps: PostgresDBOperations, tableName: String = "public.sample_dataset_range_v2")(implicit spark: SparkSession): Unit = {
    df.foreach(row =>
      pgDBOps.updatePostgresTable(tableName,
        name = row.getString(1),
        age = row.getInt(2), salary = row.getInt(3))
    )
  }

  def updateBatchRecordToPg(df: DataFrame, pgDBOps: PostgresDBOperations, tableName: String = "public.sample_dataset_range_v2")(implicit spark: SparkSession): Unit = {
    df.foreachPartition { (partition: Iterator[Row]) =>
      val batch: Iterator[(Long, String, Int, Int, Date,Timestamp,Timestamp)] = partition.map(row => (row.getLong(0), row.getString(1), row.getInt(2), row.getInt(3),row.getDate(4), row.getTimestamp(5),row.getTimestamp(6)))
      pgDBOps.upsertBatch(tableName, batch)
    }
  }

  def writeDataToPgTable(tableName: String, df: DataFrame, sparkPgOperation: SparkPostgresOperation, mode: String = "overwrite"): Unit = {
    sparkPgOperation.writeDataToTable(
      inputDF = df,
      tableName = tableName,
      mode = mode)
  }


  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder().appName("PG Connection").master("local[*]")
      .getOrCreate()
    val sparkPgOperation: SparkPostgresOperation = new SparkPostgresOperation()
    val pgDBOps: PostgresDBOperations = new PostgresDBOperations(PostgresConfigLoader.getDatabaseConfiguration())
    sparkPgOperation.getTablesList(allowedTableSchemas = Seq("public")).show(10, false)
    sparkPgOperation.getTableData("public.employees").show(10, false)


    val sampleDF = getSampleEmployeeData(100000).repartition(4)
//    writeDataToPgTable("public.sample_dataset_range_v2", sampleDF, sparkPgOperation,mode="append")
    sampleDF.printSchema()
    println("write complete")

    updateRecordToPg(sampleDF.limit(2), pgDBOps)
    updateBatchRecordToPg(sampleDF, pgDBOps)


    pgDBOps.updatePostgresTable(tableName = "public.sample_dataset_range_v2",name="Robert",age=22,salary = 41000)

    pgDBOps.createIndex(
          tableName = "public.sample_dataset_range_v2",
          indexName = "ind_sample_dataset_2",
          indexColumns = Seq("age"),
          additionalIncludeColumns = Seq("salary", "name"),
        )
//    pgDBOps.dropIndex(indexName = "ind_sample_dataset_2")


    spark.stop()


  }

}
