package processing

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataValidator extends DataValidatorTrait {

  override def validBankTransaction(accountDF: DataFrame, transactionDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    //check all target accounts are valid
    transactionDF.join(accountDF.select("AccountID"), accountDF("AccountID") === transactionDF("ToAccountID")).drop("AccountID").join(
      accountDF, accountDF("AccountID") === transactionDF("FromAccountID")
    ).filter(col("Balance") >= col("Amount")).select("TransactionID")
  }

  override def invalidBankTransaction(accountDF: DataFrame, transactionDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    accountDF
  }

  override def printTopTransaction(accountDF: DataFrame, transcationDF: DataFrame, number: Integer)(implicit spark: SparkSession): Unit = {

  }
}
