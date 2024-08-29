package helper

import org.apache.spark.sql.functions.{col, lit, rand}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object SampleDataGenerator {


  def getSampleEmployeeData(numRows: Int = 100000000)(implicit spark: SparkSession): DataFrame = {

    // Generate a DataFrame with columns id, name, age, salary
    val df: DataFrame = spark.range(1, numRows + 1).toDF("id")
      .withColumn("name", functions.concat(lit("Name_"), col("id")))
      .withColumn("age", (rand() * 80 + 1).cast("int")) // Random age between 1 and 81
      .withColumn("salary", (rand() * 50000 + 30000).cast("int"))
    df
  }

}
