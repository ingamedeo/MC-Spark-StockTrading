package com.abarag4.hw3.utils

import java.util.Date

import com.abarag4.hw3.models.Portfolio
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Random

object PolicyUtilsParallelize {

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  val random = new Random()

  def getNewRandomTicker(inputFile: Map[(Date, String), Double], portfolio: Portfolio, stockTickers: List[String]): String = {
    val ownedTickers = portfolio.getStocksMap.keys
    val newTickers = stockTickers.filterNot(ownedTickers.toSet)

    //We have already bought everything
    if (newTickers.isEmpty) {
      val result = random.nextInt(stockTickers.length)
      return stockTickers(result)
    } else {
      val result = random.nextInt(newTickers.length)
      return newTickers(result)
    }
  }

  def sellStock(inputFile: Map[(Date, String), Double], ticker: String, portfolio: Portfolio, day: Date): Double = {
    //LOG.debug("ticker: "+ ticker+ " previousPrice: "+previousPrice+ " currentPrice: "+currentPrice)
    val previousPortTuple = portfolio.getStocksMap.get(ticker)
    val currentPrice = TimeSeriesUtils.getPriceOfStockOnDay(inputFile, day, ticker)._1
    if (previousPortTuple.isEmpty) {
      LOG.debug("STOP!!! Attempted to sell stock which I don't have! ticker: "+ticker+" "+portfolio.printPortfolio("ERR"))
      throw new IllegalStateException()
      return 0.0
    }
    val previousAmount = previousPortTuple.get._2
    LOG.info("> Selling "+ day.toString + " " + previousAmount+" units of stock "+ticker+ " at price "+currentPrice+". Obtained money: "+currentPrice*previousAmount)
    return currentPrice*previousAmount
  }

  def buyStock(inputFile: Map[(Date, String), Double], ticker: String, portfolio: Portfolio, day: Date, money: Double): Double = {

    val previousPortTuple = portfolio.getStocksMap.get(ticker)
    val currentPriceT = TimeSeriesUtils.getPriceOfStockOnDay(inputFile, day, ticker)

    if (!currentPriceT._2) {
      LOG.info("Unable to buy stock "+ticker+" on day "+day+" as it is NOT traded")
      return 0.0
    }

    val currentPrice = currentPriceT._1
    val newAmount = money/currentPrice
    if (previousPortTuple.isEmpty) {
      //portfolio.getStocksMap.put(ticker, (day, newAmount))
      LOG.info("> "+ day.toString +" Buying "+newAmount+" units of stock "+ticker+ " at price "+currentPrice+". Spent money: "+newAmount*currentPrice)
      return newAmount
    } else {
      val oldAmount = previousPortTuple.get._2
      //portfolio.getStocksMap.put(ticker, (day, newAmount+oldAmount))
      LOG.info(">" + day.toString + " Buying "+newAmount+" [additional] units of stock "+ticker+ " at price "+currentPrice+". Spent money: "+newAmount*currentPrice)
      return oldAmount+newAmount
    }

  }

  def stopLoss(inputFile: Map[(Date, String), Double], ticker: String, currentPosition: (Date, Double), day: Date, delta: Double): Boolean = {

    val prevPrice = TimeSeriesUtils.getPriceOfStockOnDay(inputFile, currentPosition._1, ticker);
    val todaysPrice = TimeSeriesUtils.getPriceOfStockOnDay(inputFile, day, ticker);

    //Stock was not present at both time instants, so we can't possibly compute this
    if (!prevPrice._2 || !todaysPrice._2) {
      return false
    }

    //We are loosing money faster than delta.
    if ((prevPrice._1 - delta) < todaysPrice._1) {
      return true
    }

    return false;
  }

  def gainPlateaued(inputFile: Map[(Date, String), Double], ticker: String, currentPosition: (Date, Double), day: Date, delta: Double): Boolean = {

    val prevPrice = TimeSeriesUtils.getPriceOfStockOnDay(inputFile, currentPosition._1, ticker);
    val todaysPrice = TimeSeriesUtils.getPriceOfStockOnDay(inputFile, day, ticker);

    //Stock was not present at both time instants, so we can't possibly compute this
    if (!prevPrice._2 || !todaysPrice._2) {
      return false
    }

    //We are not making more money than delta (100 + delta (2) > 102)
    if ((prevPrice._1 + delta) > todaysPrice._1) {
      return true
    }

    return false;
  }

}
