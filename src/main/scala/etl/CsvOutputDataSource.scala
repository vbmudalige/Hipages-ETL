package etl

import org.apache.spark.sql.{DataFrame, SaveMode}

class CsvOutputDataSource extends OutputDataSource {

  /**
    * Store the dataFrame in a given location as a csv file
    *
    * @param dataFrame : dataFrame which is need to be loaded
    * @param configs   : All the configs which are required to save the data
    */
  override def saveOutputData(dataFrame: DataFrame, configs: Map[String, String]): Unit = {

    val outputPath: String = configs("outputPath")
    val partitioningCol: String = configs("partitioningCol")

    dataFrame
      .coalesce(1)
      .write
      //      .partitionBy(partitioningCol)
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(outputPath)
  }
}