package com.bigdata.spark

import com.bigdata.spark.listener.CustomSparkListener
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object HDFSJsonWriter {
  val logger: Logger = LoggerFactory.getLogger("HDFSJsonWriter")
  def main(args: Array[String]): Unit = {
    val appName = args(0)
    val configOutputFile = args(1)

    val spark = SparkSession
      .builder()
      .appName(appName)
      .getOrCreate()

    spark.sparkContext.addSparkListener(new CustomSparkListener(configOutputFile, spark, logger))
    //spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val list = List(
      "Test Test Test1 Test2 Test3",
      "Test Test Test1 Test2 Test3",
      "Test Test Test1 Test2 Test3",
      "Test Test Test1 Test2 Test3",
      "Test Test Test1 Test2 Test3"
    )

    val sentences = list.toDS
    val words = sentences.flatMap(sentence => sentence.split("\\W+"))
    val groupedWords = words.groupBy("value")

    val wordCount = groupedWords.count()
    wordCount.show()
  }
}
