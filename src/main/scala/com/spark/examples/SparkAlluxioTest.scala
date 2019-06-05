package com.spark.examples

import org.apache.spark.sql.SparkSession
/**
  * Created by mazhiyong on 19/3/20.
  */
object SparkAlluxioTest {
  def main(args: Array[String]) {
    val input = args(0)
    val sparkSession = SparkSession.builder.appName("Simple Application").getOrCreate()

    import sparkSession.implicits._
    val data = sparkSession.read.text(input).as[String]

    val words = data.flatMap(value => value.split("\\s+"))

    val groupedWords = words.groupByKey(_.toLowerCase)

    val counts = groupedWords.count()

    counts.show()
    sparkSession.stop()


  }

}
