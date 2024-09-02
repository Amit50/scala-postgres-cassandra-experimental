package executor

import helper.data.generator.SampleDataGenerator.getSampleEmployeeData
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object CassandraExperimental {

  def writeSampleToCassandra(df: DataFrame, tableName: String, keyspaceName: String): Unit = {
    df.write.format("org.apache.spark.sql.cassandra")
      .options(Map(
        "table" -> s"$tableName",
        "keyspace" -> s"$keyspaceName"
      ))
      .mode("append")
      .save()
  }

  def readCassandraTable(tableName: String, keyspaceName: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("org.apache.spark.sql.cassandra").options(Map(
      "table" -> s"$tableName",
      "keyspace" -> s"$keyspaceName"
    )).load()
  }

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder().appName("Cassandra Operations Connection").master("local[*]")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .getOrCreate()

    val sampleDF = getSampleEmployeeData(100000).filter(col("id")=== 82694).drop("salary","created_at","updated_at")
    sampleDF.show(10,false);
//    writeSampleToCassandra(sampleDF, "sample_test_data_v1", "ab_test_keyspace")
    writeSampleToCassandra(sampleDF, "sample_test_data_v2", "ab_test_keyspace")
    readCassandraTable("sample_test_data_v2", "ab_test_keyspace").filter(col("id")===900).show(100,false)
  }

}
