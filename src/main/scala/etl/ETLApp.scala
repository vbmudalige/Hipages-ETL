package hipages.sparkProject

import etl.{CsvOutputDataSource, InputDataSource, JsonInputDataSource, OutputDataSource}
import org.apache.spark.sql.SparkSession

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  * (+ select ETLLocalApp when prompted)
  */
object ETLLocalApp extends App {
  val (inputFile, outputFile, schemaFile) = (args(0), args(1), args(2))
  Runner.run(inputFile, outputFile, schemaFile)
}

/**
  * Use this when submitting the app to a cluster with spark-submit
  **/
object ETLApp extends App {
  val (inputFile, outputFile, schemaFile) = (args(0), args(1), args(2))
  Runner.run(inputFile, outputFile, schemaFile)
}

object Runner {
  def run(inputFile: String, outputFile: String, schemaFile: String): Unit = {

    implicit val spark = SparkSession
      .builder()
      .appName("ETL App")
      .config("spark.master", "local") // In a production environment, spark.master should be specified on the command line when you submit the app (ex spark-submit --master yarn)
      .getOrCreate()

    val inputConfigs = Map("inputPath" -> inputFile
      , "schemaFile" -> schemaFile
      , "charSet" -> "UTF-8")

    //TODO InputDataSource factory should be implemented to get the data source

    //Extract
    val inputDataSource: InputDataSource = new JsonInputDataSource()
    val inputDf = inputDataSource.getInputData(spark, inputConfigs)

    //Transform
    val userActivitiesDf = Transformations.getUserActivities(inputDf)
    val aggregatedEventsDf = Transformations.getAggregatedEvents(inputDf)

    //TODO OutputDataSource factory should be implemented to get the relevant data output source

    //Load
    val outputDataSource: OutputDataSource = new CsvOutputDataSource()

    //This configs should be extracted from the command line arguments in the actual implementation
    val userActivitiesOutputConfigs = Map("outputPath" -> outputFile.concat("\\userActivities\\"), "partitioningCol" -> "activity")
    val aggregateEventsOutputConfigs = Map("outputPath" -> outputFile.concat("\\aggregateEvents\\"), "partitioningCol" -> "time_bucket")

    outputDataSource.saveOutputData(userActivitiesDf, userActivitiesOutputConfigs)
    outputDataSource.saveOutputData(aggregatedEventsDf, aggregateEventsOutputConfigs)
  }
}