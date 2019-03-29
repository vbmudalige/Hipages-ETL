package etl

import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class JsonInputDataSource extends InputDataSource {

  /**
    * Extract input data from json files from a given location using a json schema
    *
    * @param spark   : SparkSession
    * @param configs : All the configs which are required to extract the data
    * @return DataFrame
    */
  override def getInputData(spark: SparkSession, configs: Map[String, String]): DataFrame = {
    val inputPath: String = configs("inputPath")
    val schemaPath: String = configs("schemaPath")
    val charSet: String = configs("charSet");

    val schemaJson = spark
      .read
      .json(schemaPath)
      .schema
      .json

    val jsonSchema = DataType
      .fromJson(schemaJson)
      .asInstanceOf[StructType]

    val inputDf = spark
      .read
      .schema(jsonSchema)
      .option("mode", "DROPMALFORMED")
      .option("charset", charSet)
      .json(inputPath)

    inputDf
  }
}
