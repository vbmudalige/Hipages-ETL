package hipages.sparkProject

/**
  * Vidura Mudalige
  */

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Transformations {
  /**
    * Transform events to user activities
    */
  def getUserActivities(df: DataFrame): DataFrame = {

    df.withColumn("_tmp", split(col("url"), "\\/"))
      .select(col("user.id"), col("timestamp"), col("_tmp").getItem(2).as("url_level1_unedited"), col("_tmp").getItem(3).as("url_level2"), col("_tmp").getItem(4).as("url_level3"), col("action"))
      .withColumn("url_level1", expr("substring(url_level1_unedited, 5, length(url_level1_unedited) - 7)"))
      .withColumnRenamed("id", "user_id")
      .withColumnRenamed("timestamp", "time_stamp")
      .withColumnRenamed("action", "activity")
      .select(col("user_id"), col("time_stamp"), col("url_level1"), col("url_level2"), col("url_level3"), col("activity"))
  }

  /**
    * Aggregate the events
    */
  def getAggregatedEvents(df: DataFrame): DataFrame = {

    df.withColumn("_tmp", split(col("url"), "\\/"))
      .select(col("user.id"), col("timestamp"), col("_tmp").getItem(2).as("url_level1_unedited"), col("_tmp").getItem(3).as("url_level2"), col("_tmp").getItem(4).as("url_level3"), col("action"))
      .withColumn("url_level1", expr("substring(url_level1_unedited, 5, length(url_level1_unedited) - 7)"))
      .withColumn("time_bucket", date_format(to_date(col("timestamp"), "dd/MM/yyyy HH:mm:ss"), "yyyyMMddHH"))
      .withColumnRenamed("id", "user_id")
      .withColumnRenamed("action", "activity")
      .select(col("user_id"), col("time_bucket"), col("url_level1"), col("url_level2"), col("url_level3"), col("activity"))
      .groupBy("time_bucket", "url_level1", "url_level2", "activity")
      .agg(count(col("activity")).alias("activity_count"), countDistinct(col("user_id")).alias("user_count"))
  }
}