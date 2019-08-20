package com.spark.examples

import org.apache.spark.sql.SparkSession
/**
  * THe LoadFile's rdd to Alluxio
  * Created by mazhiyong on 19/6/24.
  * LoadFileRddApplication
  *   -> gameplayRddAlluxio (game_id, user_id, game_type, channel_from, game_exp)
  *     -> GameUserSummaryRddAlluxioPath(game_id, user_id, play_exp, play_count)
  *       -> gameSummaryRddPath(game_id, uv, play_exp, play_count)
  *       -> gameTypeSummaryRddPath(game_type, uv, play_exp, play_count)
  */
object SparkSQLGBDSTest {
  def main(args: Array[String]): Unit = {
    val gamePlayInputFile = args(0)
    val gamePlayRddPath= args(1)
    val spark = SparkSession.builder.appName("Simple SparkSQL GBDS Test").getOrCreate()
    val dataFrame = spark.read.json(gamePlayInputFile)
    dataFrame.createTempView("game_play")
    val gamePlayDF = spark.sql("select game_id, user_id, game_type, channel_from, game_exp from game_play")
    gamePlayDF.rdd.saveAsTextFile(gamePlayRddPath)
    spark.stop()
  }
}