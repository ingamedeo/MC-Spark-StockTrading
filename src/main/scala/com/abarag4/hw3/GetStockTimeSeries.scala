package com.abarag4.hw3

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}

import com.abarag4.hw3.StockSimulatorParallelize.{LOG, configuration}
import com.abarag4.hw3.utils.{TickerUtils, TimeSeriesUtils}

import scala.io.Source
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object GetStockTimeSeries {

  val QUOTE = "\""

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)


  def checkRetrData(response: String): Boolean = {
    if (response.contains("{")) {
      return false
    }
    return true
  }

  def retrieveTickerTimeSeries(tickerName: String, apiKey: String): Unit = {

    val extension = ".csv"
    val path = configuration.getString("configuration.tickerDownloadPath")

    val file = new File(path+tickerName+extension)
    if (file.isFile) {
      LOG.debug("Skipping time series data for "+tickerName)
      return
    }

    try {
        LOG.debug("Now retrieving time series data for "+tickerName)
        val content = get("https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol="+tickerName+"&outputsize=full&apikey="+apiKey+"&datatype=csv")

      if (!checkRetrData(content)) {
        LOG.debug("Failed to retrieve time series because we exceeded API limits. Waiting.");

        //Wait 5 seconds
        Thread.sleep(5001)

        LOG.debug("Trying again to retrieve time series data for "+tickerName);

        retrieveTickerTimeSeries(tickerName, apiKey)
        return
      }

      val buffWriter = new BufferedWriter(new FileWriter(file))
        buffWriter.write(content)
        buffWriter.close()
      } catch {
        case ioe: java.io.IOException => ioe.printStackTrace()
        case ste: java.net.SocketTimeoutException => ste.printStackTrace()
      }
  }

  def retrieveTickersTimeSeries(tickerList: scala.collection.mutable.ListBuffer[String]): Unit = {
    tickerList.foreach(el => retrieveTickerTimeSeries(el, "0T7FFMR6IO8H1OL0"))
  }

  @throws(classOf[java.io.IOException])
  def get(url: String): String = {
    val source = Source.fromURL(url)
    val urlStr = source.mkString
    source.close()
    return urlStr
  }

  def main(args: Array[String]): Unit = {

    val jobName: String = configuration.getString("configuration.jobName")
    val tickerFile: String = configuration.getString("configuration.tickerFile")
    val tickerDownloadPath: String = configuration.getString("configuration.tickerDownloadPath")
    val mergedFile: String = configuration.getString("configuration.mergedFile")

    val sparkConf = new SparkConf().setAppName(jobName).setMaster("local")
    val context = new SparkContext(sparkConf)

    val tickerList = scala.collection.mutable.ListBuffer.empty[String]

    TickerUtils.createTickerList(tickerList, tickerFile)

    retrieveTickersTimeSeries(tickerList)

    /* Merge newly downloaded API data if we haven't already done so */
    val timeSeriesInputFiles = context.wholeTextFiles(tickerDownloadPath)

    if (!Files.exists(Paths.get(mergedFile))) {
      LOG.info("Merging time series data..")
      TimeSeriesUtils.processTimeSeriesFiles(timeSeriesInputFiles, mergedFile)
    } else {
      LOG.info("Time series data already merged!")
    }

  }
}
