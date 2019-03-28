package hipages.sparkProject

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  * (+ select ETLLocalApp when prompted)
  */
object ETLLocalApp extends App {
  val (inputFile, outputFile) = (args(0), args(1))
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("my awesome app")

  Runner.run(conf, inputFile, outputFile)
}

/**
  * Use this when submitting the app to a cluster with spark-submit
  **/
object ETLApp extends App {
  val (inputFile, outputFile) = (args(0), args(1))

  // spark-submit command should supply all necessary config elements
  Runner.run(new SparkConf(), inputFile, outputFile)
}

object Runner {
  def run(conf: SparkConf, inputFile: String, outputFile: String): Unit = {
    val sc = new SparkContext(conf)

    //Extract
    val inputDf: DataFrame = sc
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
