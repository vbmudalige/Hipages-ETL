package hipages.sparkProject

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  * (+ select ETLLocalApp when prompted)
  */
object ETLLocalApp extends App {
  val (inputFile, outputFile) = (args(0), args(1))
  Runner.run(inputFile, outputFile)
}

/**
  * Use this when submitting the app to a cluster with spark-submit
  **/
object ETLApp extends App {
  val (inputFile, outputFile) = (args(0), args(1))

  // spark-submit command should supply all necessary config elements
  Runner.run(inputFile, outputFile)
}

object Runner {
  def run(inputFile: String, outputFile: String): Unit = {

    implicit val spark = SparkSession.builder()
      .enableHiveSupport()
      .appName("ETL App")
      .getOrCreate()

    //Extract
    val inputDf: DataFrame = spark
      .read
      // .schema(theSchema)
      .option("mode", "DROPMALFORMED")
      .option("charset", "UTF-8")
      .json(inputFile)

    //Transform
    val transformedDf = Transformations.getUserActivities(inputDf)

    //Load
    transformedDf
      .write
      .option("header", "true")
      .csv(outputFile)
  }
}
