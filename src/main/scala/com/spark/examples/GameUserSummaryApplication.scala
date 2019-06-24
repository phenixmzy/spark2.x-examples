package com.spark.examples

import org.apache.spark.sql.SparkSession

/**
  * THe User Game Summary Job data is read rdd from alluxio
  * Created by mazhiyong on 19/6/24.
  */
object GameUserSummaryApplication {
  def main(args: Array[String]): Unit = {
    val gameplayRddAlluxioPath = args(0)
    val gameUserSummaryRddAlluxioPath = args(1)
    val spark = SparkSession.builder.appName("Simple Application Read RDD From Alluxio").getOrCreate()
    val dataFrame = spark.read.load(gameplayRddAlluxioPath)
    dataFrame.createTempView("game_play")
    val userGameSummaryDF = spark.sql("select game_id, user_id, sum(game_exp) play_exp, count(1) play_exp from game_play group by game_id, user_id")
    userGameSummaryDF.rdd.saveAsTextFile(gameUserSummaryRddAlluxioPath)
    spark.stop()
  }
}