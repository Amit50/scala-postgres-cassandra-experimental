package reader

import org.apache.spark.sql.{DataFrame, SparkSession}

object FileReader {

  def getFileDataFrame(
                        format: String,
                        path: String,
                        optionsMap: Map[String, String]
                      )(implicit spark: SparkSession): DataFrame = {
    format.toLowerCase() match {
      case csv => spark.read.format("csv").options(optionsMap).load(path)
    }
  }

  def getAbsolutePathForResource(resourcePath: String): String = {
    getClass.getResource(resourcePath).getPath
  }

}
