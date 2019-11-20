package com.abarag4.hw3

import java.io.{BufferedWriter, File, FileWriter}

import com.abarag4.hw3.GetStockTimeSeries.configuration
import com.abarag4.hw3.StockSimulator.{configuration, getClass}
import com.abarag4.hw3.utils.{TickerUtils, TimeSeriesUtils}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text

class StockJob(context: SparkContext) {

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)



  /*
  def kmeans() = {
    // Load and parse the data
    val data = context.textFile("data/mllib/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    val distr = clusters.predict(parsedData)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
  }
   */

  /*
  Sell, Buy, DoNothing

  Delta > 1% -> Buy/Sell ->> distribution
  */

  //column 0, column 5
  //ABC, data, adj_closing_value

    //RDD[(String, String)]
  def run(tickerFileName: String, numberOfStocks: Int, initialAmount: Int) : Double = {

    LOG.debug("*** Starting StockJob ***")

    val outputFile = "data/output.csv"

    //val fNameContent = context.wholeTextFiles("jsons/")
    //TimeSeriesUtils.processTimeSeriesFiles(fNameContent, outputFile)

    //val timePeriodStart: String = configuration.getString("configuration.timePeriodStart")
    //val timePeriodEnd: String = configuration.getString("configuration.timePeriodEnd")

    //TimeSeriesUtils.filterByDate(context, outputFile, timePeriodStart, timePeriodEnd)

    //val stocksList = generateRandomStockList(tickerFileName, numberOfStocks)

    //Print
    //stocksList.foreach(println)

    return 0.0
  }
}
