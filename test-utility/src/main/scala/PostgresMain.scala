import database.connector.PostgresConfigLoader
import database.operations.{PostgresDBOperations, SparkPostgresOperation}
import helper.data.generator.SampleDataGenerator.getSampleEmployeeData
import org.apache.spark.sql.{Row, SparkSession}

object PostgresMain {


  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder().appName("PG Connection").master("local[*]")
      .getOrCreate()
    val sparkPgOperation: SparkPostgresOperation = new SparkPostgresOperation()
    val pgDBOps: PostgresDBOperations = new PostgresDBOperations(PostgresConfigLoader.getDatabaseConfiguration())
    //    sparkPgOperation.getTablesList(allowedTableSchemas = Seq("public")).show(10, false)
    //    sparkPgOperation.getTableData("public.employees").show(10, false)
//    sparkPgOperation.writeDataToTable(
//      inputDF = getSampleEmployeeData(100000),
//      tableName = "public.sample_dataset_range_v2",
//      mode = "append")

    val sampleDF = getSampleEmployeeData(1000)
    sampleDF.printSchema()
//    sampleDF.write.format("org.apache.spark.sql.cassandra")
//      .options(Map(
//        "table" -> "sample_test_data_v1",
//        "keyspace" -> "ab_test_keyspace"
//      ))
//      .mode("append")
//      .save()

    sampleDF.write.format("org.apache.spark.sql.cassandra")
      .options(Map(
        "table" -> "sample_test_data_v2",
        "keyspace" -> "ab_test_keyspace"
      ))
      .mode("append")
      .save()

    print("write complete")
    //    getSampleEmployeeData(2).foreach(row =>
    //      pgDBOps.updatePostgresTable(tableName = "public.sample_dataset_range_v1",
    //        name = row.getString(1),
    //        age = row.getInt(2), salary = row.getInt(3))
    //    )
    //    getSampleEmployeeData(1000).foreachPartition { (partition: Iterator[Row]) =>
    //      val batch: Iterator[(Long, String, Int, Int)] = partition.map(row => (row.getLong(0), row.getString(1), row.getInt(2), row.getInt(3)))
    //      pgDBOps.upsertBatch(tableName = "public.sample_dataset_range_v1", batch)
    //    }


    //    PostgresDBOperations.updatePostgresTable(tableName = "public.sample_dataset_range_v1",age=22,salary = 41000,dbConfig = PostgresConfigLoader.getDatabaseConfiguration())

    //    PostgresDBOperations.createIndex(
    //      tableName = "public.sample_dataset_1",
    //      indexName = "index_sample_dataset_1",
    //      indexColumns = Seq("age"),
    //      additionalIncludeColumns = Seq("salary", "name"),
    //      dbConfig = PostgresConfigLoader.getDatabaseConfiguration()
    //    )

    //    PostgresDBOperations.dropIndex(
    //      indexName = "grades_pkey",
    //      dbConfig = PostgresConfigLoader.getDatabaseConfiguration()
    //    )


    spark.stop()


  }

}
