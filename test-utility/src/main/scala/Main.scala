package com.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import processing.DataValidator
import reader.FileReader
import reader.FileReader.getAbsolutePathForResource
import org.apache.log4j.{Level, Logger}
//import org.apache.spark.S

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("*").setLevel(Level.WARN)
    implicit val spark: SparkSession = SparkSession.builder().appName("DataValidator").master("local[1]").getOrCreate()
    println(spark.conf.getAll)
    val dataValidator = new DataValidator()
    val accountDF = FileReader.getFileDataFrame(format = "CSV", path = getAbsolutePathForResource("/accounts.csv"),
      optionsMap = Map("header" -> "true", "inferSchema" -> "true")
    )
    val transactionDF = FileReader.getFileDataFrame(format = "CSV", path = getAbsolutePathForResource("/transactions.csv"),
      optionsMap = Map("header" -> "true", "inferSchema" -> "true")
    )
    accountDF.show(10)
    transactionDF.show(10)
    dataValidator.validBankTransaction(accountDF,transactionDF).show(100,false)
    println("Hello from Scala Maven Submodule!")
    Thread.sleep(100*1000)
  }




}

