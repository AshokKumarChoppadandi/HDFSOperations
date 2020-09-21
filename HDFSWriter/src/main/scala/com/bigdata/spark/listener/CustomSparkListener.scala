package com.bigdata.spark.listener

import java.io.{BufferedOutputStream, File, FileInputStream, PrintWriter}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession
import org.json.JSONObject
import org.slf4j.Logger

import scala.collection.mutable

class CustomSparkListener(configOutputFileLocation: String, spark: SparkSession, logger: Logger) extends SparkListener {

  //val jsonStringTemp = "{\"id\":123,\"name\":\"Test\",\"salary\":10000,\"dept\":\"IT\"}"
  //val outputLocation = "file:///Users/achoppadandi/Documents/Ashok/hadoop/test.json"

  val configProperties: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logger.info("Application Completed and trying to write the Metrics")
    logger.info("Output file location - ", configOutputFileLocation)
    //logger.info("Metrics JSON - ", jsonStringTemp)

    val jsonConfig = new JSONObject()

    val configs = spark.sparkContext.getConf.getAll
    configs.foreach(config => jsonConfig.put(config._1, config._2))

    val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
    hadoopConfiguration.setBoolean("dfs.support.append", true)

    val fs = FileSystem.get(hadoopConfiguration)
    val filePath = new Path(configOutputFileLocation)
    val fileOutputStream = if(fs.exists(filePath)) {
      logger.warn("File already exists, trying to append metrics")
      fs.append(filePath)
    } else {
      logger.info("New metrics file created")
      fs.create(filePath)
    }

    logger.info("Writing the metrics to the file")
    val printWriter = new PrintWriter(fileOutputStream)
    //printWriter.write(jsonStringTemp + "\n")
    logger.info("Metrics JSON - ", jsonConfig.toString)
    printWriter.write(jsonConfig.toString)

    printWriter.flush()
    fileOutputStream.hflush()

    printWriter.close()
    fileOutputStream.close()
    logger.info("Metrics written successfully to the output location")

    /*val outputStream = new BufferedOutputStream(fileOutputStream)
    val byteArray = jsonStringTemp.getBytes()
    outputStream.write(byteArray, 0, byteArray.length)

    outputStream.flush()
    outputStream.close()*/

    //val byteArray = jsonStringTemp.getBytes()
    /*fileOutputStream.writeBytes(jsonStringTemp)

    fileOutputStream.flush()
    fileOutputStream.close()*/

  }
}
