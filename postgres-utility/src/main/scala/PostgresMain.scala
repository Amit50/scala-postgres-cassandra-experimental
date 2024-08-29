import database.operations.{PostgresDBOperations, SparkPostgresOperation}
import helper.SampleDataGenerator.getSampleEmployeeData
import org.apache.spark.sql.SparkSession

object PostgresMain {


  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder().appName("PG Connection").master("local[*]")
      .config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()
    val sparkPgOperation: SparkPostgresOperation = new SparkPostgresOperation()
//    sparkPgOperation.getTableData("public.employees").show(10, false)

    sparkPgOperation.getTablesList(allowedTableSchemas = Seq("public")).show(10, false)
    sparkPgOperation.writeDataToTable(getSampleEmployeeData(100000), "public.sample_dataset_1")

    PostgresDBOperations.dropIndex("")


    spark.stop()


  }

}
