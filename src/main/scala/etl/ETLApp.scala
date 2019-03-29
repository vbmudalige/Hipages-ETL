package hipages.sparkProject

import etl.{CsvOutputDataSource, JsonInputDataSource, OutputDataSource}
import org.apache.spark.sql.SparkSession

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  * (+ select ETLLocalApp when prompted)
  */
object ETLLocalApp extends App {
  val (inputFile, outputFile, schemaPath) = (args(0), args(1), args(2))
  Runner.run(inputFile, outputFile, schemaPath)
}

/**
  * Use this when submitting the app to a cluster with spark-submit
  **/
object ETLApp extends App {
  val (inputFile, outputFile, schemaPath) = (args(0), args(1), args(2))

  // spark-submit command should supply all necessary config elements
  Runner.run(inputFile, outputFile, schemaPath)
}

object Runner {
  def run(inputFile: String, outputFile: String, schemaPath: String): Unit = {

    implicit val spark = SparkSession.builder()
      .enableHiveSupport()
      .appName("ETL App")
      .getOrCreate()

    //This configs map should be provided as an external parameter to the jar in the actual implementation
    val inputConfigs = Map("inputPath" -> inputFile
      , "schemaPath" -> schemaPath
      , "charSet" -> "UTF-8")

    //TODO InputDataSource factory should be implemented to get the data source

    //Extract
    val inputDf = new JsonInputDataSource().getInputData(spark, inputConfigs)

    //Transform
    val userActivitiesDf = Transformations.getUserActivities(inputDf)
    val aggregatedEventsDf = Transformations.getAggregatedEvents(inputDf)

    //TODO OutputDataSource factory should be implemented to get the relevant data output source

    //Load
    val outputDataSource: OutputDataSource = new CsvOutputDataSource()

    //This configs map should be provided as an external parameter to the jar in the actual implementation
    val outputConfigs = Map("outputPath" -> outputFile)

    outputDataSource.saveOutputData(userActivitiesDf, outputConfigs)
    outputDataSource.saveOutputData(aggregatedEventsDf, outputConfigs)
  }
}