package com.abarag4.hw3

import java.nio.file.{Files, Paths}
import java.util.Date

import com.abarag4.hw3.StockSimulator.getClass
import com.abarag4.hw3.models.Portfolio
import com.abarag4.hw3.utils.{PolicyUtils, PolicyUtilsParallelize, TickerUtils, TimeSeriesUtils}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

object StockSimulatorParallelize {
  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  def buySellPolicy(portfolio: Portfolio, inputFile: Map[(Date, String), Double], stockTickers: List[String], initialMoney: Double, days: List[Date], currentDayIndex: Int): Unit = {

    /*
    * Buy an equal portion of each stock to begin with
     */

    //Base case
    if (currentDayIndex >= days.size) {
      return
    }

    //Get current day
    val day = days(currentDayIndex)

    //Perform first action
    if (currentDayIndex==0) {
      stockTickers.foreach(t => {
        val moneyToInvestForStock: Double = initialMoney/stockTickers.length
        val amount = PolicyUtilsParallelize.buyStock(inputFile, t, portfolio, day, moneyToInvestForStock)
        if (amount!=0.0) {
          portfolio.getStocksMap.put(t, (day, amount))
        }
      })

      portfolio.printPortfolio("initial")

      return buySellPolicy(portfolio, inputFile, stockTickers, initialMoney, days, currentDayIndex+1)
    }

    val newPortfolio: Portfolio = new Portfolio

    //Copy elements between portfolios
    portfolio.getStocksMap.foreach(s => newPortfolio.stocksMap.put(s._1, s._2))

    portfolio.printPortfolio("oldPort")
    newPortfolio.printPortfolio("newPort")

    //For each ticker that I own
    portfolio.getStocksMap.keys.foreach(ticker => {

      val previousPortTuple = portfolio.getStocksMap.get(ticker)

      val previousPrice = TimeSeriesUtils.getPriceOfStockOnDay(inputFile, previousPortTuple.get._1, ticker)._1
      val currentPrice = TimeSeriesUtils.getPriceOfStockOnDay(inputFile, day, ticker)._1
      LOG.debug("ticker: "+ ticker+ " previousPrice: "+previousPrice+ " currentPrice: "+currentPrice)

      if (PolicyUtilsParallelize.stopLoss(inputFile,ticker, previousPortTuple.get, day, 2)) {
        LOG.debug("stopLoss for ticker "+ticker+" at day "+day.toString)
        val money = PolicyUtilsParallelize.sellStock(inputFile, ticker, newPortfolio, day)
        newPortfolio.getStocksMap.remove(ticker)
        val newTicker = PolicyUtilsParallelize.getNewRandomTicker(inputFile, portfolio, stockTickers)
        val amount = PolicyUtilsParallelize.buyStock(inputFile, newTicker, newPortfolio, day, money)
        if (amount!=0.0) {
          newPortfolio.getStocksMap.put(newTicker, (day, amount))
        }

        newPortfolio.printPortfolio(day.toString)
        return buySellPolicy(newPortfolio, inputFile, stockTickers, initialMoney, days, currentDayIndex+1)

      }

      if (PolicyUtilsParallelize.gainPlateaued(inputFile, ticker, previousPortTuple.get, day, 0.1)) {
        LOG.debug("gainPlateaued for ticker "+ticker+" at day "+day.toString)
        val money = PolicyUtilsParallelize.sellStock(inputFile, ticker, newPortfolio, day)
        newPortfolio.getStocksMap.remove(ticker)
        val newTicker = PolicyUtilsParallelize.getNewRandomTicker(inputFile, portfolio, stockTickers)
        val amount = PolicyUtilsParallelize.buyStock(inputFile, newTicker, newPortfolio, day, money)
        if (amount!=0.0) {
          newPortfolio.getStocksMap.put(newTicker, (day, amount))
        }
        newPortfolio.printPortfolio(day.toString)
        return buySellPolicy(newPortfolio, inputFile, stockTickers, initialMoney, days, currentDayIndex+1)
      }

    })

    //stockTickers.foreach(t => println("day: "+ day+ " stock: "+ t  + " " + getAmountOfStockOnDay(inputFile, day, t)))
  }

  //This function retrieves the list of ordered days in the inputData (within the time period specified)
  def getListOfOrderedDays(inputData: RDD[String]) : List[Date] = {
    val daysList = scala.collection.mutable.ListBuffer.empty[Date]

    val days = inputData.map(f => TimeSeriesUtils.getDateFromLine(f)).distinct
    days.collect().sorted.foreach(d => daysList.append(d))

    return daysList.toList
  }

  def startSimulation(sim: Int, tickers: List[String], inputData: Map[(Date, String), Double], days: List[Date], initialMoney: Double) : Double = {

    LOG.info("Starting simulation now!")
    LOG.info("Simulation number: "+sim)

    //val actionsList = scala.collection.mutable.ListBuffer.empty[Portfolio]

    val emptyPortfolio = new Portfolio

    //days.foreach(day => buySellPolicy(emptyPortfolio, inputData, tickers, day, initialMoney))
    buySellPolicy(emptyPortfolio, inputData, tickers, initialMoney, days, 0)

    LOG.info("Simulation finished")

    //print each action
    //actionsList.foreach(x => println(x))

    return 0.0
  }

  def generateRandomStockList(context: SparkContext, inputFile: String,  numberOfStocks: Int): RDD[String] = {
    val tickerFile = context.textFile(inputFile)
    val sampledLines = tickerFile.takeSample(false, numberOfStocks)
    val rdd1 = context.parallelize(sampledLines)
    return rdd1.map(l => TickerUtils.getTickerName(l))
  }

  def main(args: Array[String]): Unit = {

    val jobName: String = configuration.getString("configuration.jobName")
    val initialMoney: Double = configuration.getDouble("configuration.initialAmount")
    val timePeriodStart: String = configuration.getString("configuration.timePeriodStart")
    val timePeriodEnd: String = configuration.getString("configuration.timePeriodEnd")
    val tickerFile: String = configuration.getString("configuration.tickerFile")
    val mergedFile: String = configuration.getString("configuration.mergedFile")
    val numberOfStocks: Int = configuration.getInt("configuration.numberOfStocks")
    val numberOfSims: Int = configuration.getInt("configuration.numSimulations")

    LOG.info("Setting up Spark environment..")

    val sparkConf = new SparkConf().setAppName(jobName).setMaster("local")
    val context = new SparkContext(sparkConf)

    /* Generate random start tickers */
    val initialTickers = generateRandomStockList(context, tickerFile, numberOfStocks)

    LOG.info("Random stock portfolio OK")

    /* Merge newly downloaded API data if we haven't already done so */
    val timeSeriesInputFiles = context.wholeTextFiles("jsons/")

    if (!Files.exists(Paths.get(mergedFile))) {
      LOG.info("Merging time series data..")
      TimeSeriesUtils.processTimeSeriesFiles(timeSeriesInputFiles, mergedFile)
    } else {
      LOG.info("Time series data already merged!")
    }

    /* Filter merged input file by date */
    val filteredInput = TimeSeriesUtils.filterByDate(context, mergedFile, timePeriodStart, timePeriodEnd)
    LOG.info("Time series data filtering complete!")

    val inputMap = filteredInput.map(x => {
      val line = x.split(Constants.COMMA)
      ((TimeSeriesUtils.getDateFromLine(x), line(0)), line(2).toDouble)
    }).cache()

    LOG.info("Time series data preparation..")
    val days = getListOfOrderedDays(filteredInput)
    LOG.info("Time series data preparation complete!")

    //Convert RDD to usual Scala types as within a single simulation no parallelism is present.
    val initialTickersList = initialTickers.collect.toList
    val filteredInputList = inputMap.collect.toMap[(Date, String), Double]


      val finalAmounts = (context.parallelize(1 to numberOfSims)
      .map(i => startSimulation(i, initialTickersList, filteredInputList, days, initialMoney))
      .reduce(_+_)/numberOfSims.toDouble)


    context.stop()
  }
}