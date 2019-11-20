package com.abarag4.hw3.models

import java.util.Date

import com.abarag4.hw3.StockSimulator.getClass
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}

@SerialVersionUID(100L)
class Portfolio extends Serializable {

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  //Key: Ticker Value: (StockPrice, StockAmount)
  val stocksMap = scala.collection.mutable.Map.empty[String, (Date, Double)]

  def getStocksMap: scala.collection.mutable.Map[String, (Date, Double)] = {
    return stocksMap
  }

  def printPortfolio(des: String): Unit = {

    LOG.info("*** Portfolio "+des+ " ***")
    stocksMap.foreach(stock => {
      LOG.info("Stock: "+stock._1+" dayBought: "+stock._2._1+ " stockAmount: "+stock._2._2)
    })
    LOG.info("*** END Portfolio "+des+ " ***")
  }
}