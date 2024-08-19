package processing

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataValidatorTrait {


  def validBankTransaction(accountDF: DataFrame,transactionDF: DataFrame)(implicit spark: SparkSession): DataFrame

  def invalidBankTransaction(accountDF: DataFrame,transactionDF: DataFrame)(implicit spark: SparkSession): DataFrame

  def printTopTransaction(accountDF: DataFrame,transcationDF: DataFrame,number: Integer)(implicit spark: SparkSession): Unit

}
