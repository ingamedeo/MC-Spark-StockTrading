package com.abarag4.hw3

import com.abarag4.hw3.StockSimulator.getClass
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object WordCount {

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  //Read input and output file paths from configuration file
  val inputFile: String = configuration.getString("configuration.inputFile")
  val outputFile: String = configuration.getString("configuration.outputFile")

  def main(args: Array[String]): Unit = {

    val jobName: String = configuration.getString("configuration.jobName")

    val sparkConf = new SparkConf().setAppName(jobName).setMaster("local")
    val context = new SparkContext(sparkConf)

    val textFile = context.textFile(inputFile)
    val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile(outputFile)

    context.stop()
  }

}
