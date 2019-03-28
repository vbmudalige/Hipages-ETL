package hipages.sparkProject

/**
  * Vidura Mudalige
  */

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class TransformationsTest extends FunSuite with SharedSparkContext {
  test("transform events to user activities") {
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val sc = spark.sparkContext
    val eventsRdd = sc.parallelize(Seq(("893479324983546", ("564561", 56456, "111.222.333.4"), "page_view", "https://www.hipages.com.au/articles", "02/02/2017 20:22:00"),
      ("349824093287032", ("564562", 56456, "111.222.333.5"), "page_view", "https://www.hipages.com.au/connect/sfelectrics/service/190625", "02/02/2017 20:23:00"),
      ("324872349721534", ("564563", 56456, "111.222.33.66"), "page_view", "https://www.hipages.com.au/get_quotes_simple?search_str=sfdg", "02/02/2017 20:26:00")))

    val eventsDf = spark.createDataFrame(eventsRdd).toDF("event_id", "user", "action", "url", "timestamp")
    val userActivitiesDf = Transformations.getUserActivities(eventsDf)
    assert(userActivitiesDf == null)
  }
}
