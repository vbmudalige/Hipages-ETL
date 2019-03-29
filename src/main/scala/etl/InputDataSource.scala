package etl

import org.apache.spark.sql.{DataFrame, SparkSession}

trait InputDataSource {
    def getInputData(spark: SparkSession, configs: Map[String, String]): DataFrame
}
