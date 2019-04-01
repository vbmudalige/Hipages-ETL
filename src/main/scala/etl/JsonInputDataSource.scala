package etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.zalando.spark.jsonschema.SchemaConverter

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
    val schemaPath: String = configs("schemaFile")
    val charSet: String = configs("charSet")

    val schema = SchemaConverter.convert(schemaPath)

    val inputDf = spark
      .read
      .schema(schema)
      .option("mode", "DROPMALFORMED")
      .option("charset", charSet)
      .json(inputPath)

    inputDf
  }
}
