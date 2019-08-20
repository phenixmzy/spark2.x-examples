package com.spark.examples

import org.apache.spark.sql.SparkSession

/**
  * Created by mazhiyong on 19/6/24.
  */
object SparkSQLGTSTest {
  def main(args: Array[String]): Unit = {
    val gameplayRddAlluxioPath = args(0)
    val gameUserSummaryRddAlluxioPath = args(1)
    val spark = SparkSession.builder.appName("Simple SparkSQL GTS Test").getOrCreate()
    val dataFrame = spark.read.load(gameplayRddAlluxioPath)
    dataFrame.createTempView("game_play")
    val gameTypeSummaryDF = spark.sql("select game_type, count(distinct user_id) uv, sum(game_exp) play_exp, count(1) play_exp from game_play group by game_type")
    gameTypeSummaryDF.rdd.saveAsTextFile(gameUserSummaryRddAlluxioPath)
    spark.stop()
  }
}
