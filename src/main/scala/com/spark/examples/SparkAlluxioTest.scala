package com.spark.examples
import org.apache.spark.sql.SparkSession
/**
  * Created by mazhiyong on 19/7/5.
  */
object SparkAlluxioTest {
  def main(args: Array[String]) {
    val input = args(0)
    val sparkSession = SparkSession.builder.appName("Simple Application").getOrCreate()

    import sparkSession.implicits._
    val words = sparkSession.read.text(input).as[String]
      .flatMap(value => value.split("\\s+"))
      .groupByKey(_.toLowerCase)

    val counts = words.count()

    counts.show()
    sparkSession.stop()
  }
}
