package etl

import org.apache.spark.sql.DataFrame

trait OutputDataSource {
  def saveOutputData(dataFrame: DataFrame, configs: Map[String, String]): Unit
}