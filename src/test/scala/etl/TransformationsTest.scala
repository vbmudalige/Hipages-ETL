package hipages.sparkProject

/**
  * Vidura Mudalige
  */

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class TransformationsTest extends FunSuite with SharedSparkContext {

  //TODO all the test cases should be added
  test("transform events to user activities") {
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    import spark.implicits._

    val eventsDf = Seq(Source("893479324983546", User("564561", "56456", "111.222.333.4"), "page_view", "https://www.hipages.com.au/articles", "02/02/2017 20:22:00"),
      Source("349824093287032", User("564562", "56456", "111.222.333.5"), "page_view", "https://www.hipages.com.au/connect/sfelectrics/service/190625", "02/02/2017 20:23:00"),
      Source("324872349721534", User("564563", "56456", "111.222.33.66"), "page_view", "https://www.hipages.com.au/get_quotes_simple?search_str=sfdg", "02/02/2017 20:26:00")).toDF

    val actualDf = Transformations.getUserActivities(eventsDf)

    val expectedDf = Seq(Activity("56456", "02/02/2017 20:22:00", "hipages.com", "articles", null, "page_view"),
      Activity("56456", "02/02/2017 20:23:00", "hipages.com", "connect", "sfelectrics", "page_view"),
      Activity("56456", "02/02/2017 20:26:00", "hipages.com", "get_quotes_simple?search_str=sfdg", null, "page_view")).toDF

    val actual = actualDf.collect
    val expected = expectedDf.collect

    assert(actual.sameElements(expected))
  }
}

case class User(session_id: String, id: String, ip: String)

case class Source(event_id: String, user: User, action: String, url: String, timestamp: String)

case class Activity(user_id: String, time_stamp: String, url_level1: String, url_level2: String, url_level3: String, activity: String)