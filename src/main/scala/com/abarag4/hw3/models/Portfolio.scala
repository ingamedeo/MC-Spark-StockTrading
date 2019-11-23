package com.abarag4.hw3.models

import java.util.Date

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Portfolio model for the simulations. This class contains a map where the Key is the Stock (Ticker) and the value is a tuple (StockPrice, StockAmount)
 * The StockPrice refers to the day the stock has been bought. The amount is a Double, partial stocks can be bought. (This is true also in the real world)
 */
@SerialVersionUID(100L)
class Portfolio extends Serializable {

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  //Key: Ticker Value: (StockPrice, StockAmount)
  val stocksMap = scala.collection.mutable.Map.empty[String, (Date, Double)]

  /**
   * Getter for StockMap
   * @return
   */
  def getStocksMap: scala.collection.mutable.Map[String, (Date, Double)] = {
    return stocksMap
  }

  /**
   * Utility function to print the Portfolio information, mainly used for debugging purposes.
   * @param des Portfolio description (optional parameter)
   */
  def printPortfolio(des: String): Unit = {

    LOG.info("*** Portfolio "+des+ " ***")
    stocksMap.foreach(stock => {
      LOG.info("Stock: "+stock._1+" dayBought: "+stock._2._1+ " stockAmount: "+stock._2._2)
    })
    LOG.info("*** END Portfolio "+des+ " ***")
  }
}