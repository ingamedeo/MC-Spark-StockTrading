package com.abarag4.hw3

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Date

import com.abarag4.hw3.StockSimulator.getClass
import com.abarag4.hw3.models.Portfolio
import com.abarag4.hw3.utils.{PolicyUtils, PolicyUtilsParallelize, TickerUtils, TimeSeriesUtils}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

object StockSimulatorParallelize {
  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)
  val sdf = new SimpleDateFormat("yyyy/MM/dd")

  /**
   * This function implements the chosen policy for the simulations; in order to do so, it relies on other utility functions.
   * Please see the comments above the utility functions for more details.
   *
   * @param sim Current simulation index
   * @param portfolioOutput Portfolio generated after the policy performs the appropriate actions on the input data
   * @param portfolio Current portfolio
   * @param inputFile Map containing the input data
   * @param stockTickers List of tickers used by the current set of simulations
   * @param initialMoney Initial amount of money available
   * @param days List of days in the specified time period
   * @param currentDayIndex Index of the current day in the List of days.
   */
  def buySellPolicy(sim: Int, portfolioOutput: ListBuffer[(String, Double)], portfolio: Portfolio, inputFile: Map[(Date, String), Double], stockTickers: List[String], initialMoney: Double, days: List[Date], currentDayIndex: Int): Unit = {

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

      //portfolio.printPortfolio("initial")

      return buySellPolicy(sim, portfolioOutput, portfolio, inputFile, stockTickers, initialMoney, days, currentDayIndex+1)
    }

    val newPortfolio: Portfolio = new Portfolio

    //Copy elements between portfolios
    portfolio.getStocksMap.foreach(s => newPortfolio.stocksMap.put(s._1, s._2))

    //portfolio.printPortfolio("oldPort")
    //newPortfolio.printPortfolio("newPort")

    //For each ticker that I own
    portfolio.getStocksMap.keys.foreach(ticker => {

      val previousPortTuple = portfolio.getStocksMap.get(ticker)

      val previousPrice = TimeSeriesUtils.getPriceOfStockOnDay(inputFile, previousPortTuple.get._1, ticker)._1
      val currentPrice = TimeSeriesUtils.getPriceOfStockOnDay(inputFile, day, ticker)._1
      //***LOG.debug("ticker: "+ ticker+ " previousPrice: "+previousPrice+ " currentPrice: "+currentPrice)

      if (PolicyUtilsParallelize.stopLoss(inputFile,ticker, previousPortTuple.get, day, 2)) {
        //***LOG.debug("stopLoss for ticker "+ticker+" at day "+day.toString)
        val money = PolicyUtilsParallelize.sellStock(inputFile, ticker, newPortfolio, day)
        newPortfolio.getStocksMap.remove(ticker)
        val tuple = checkAvailabilityAndBuy(portfolio, newPortfolio, inputFile, stockTickers, day, money)
        if (tuple._2!=0.0) {
          newPortfolio.getStocksMap.put(tuple._1, (day, tuple._2))
        }
        //newPortfolio.printPortfolio(day.toString)
      } else if (PolicyUtilsParallelize.gainPlateaued(inputFile, ticker, previousPortTuple.get, day, 0.1)) {
        //***LOG.debug("gainPlateaued for ticker "+ticker+" at day "+day.toString)
        val money = PolicyUtilsParallelize.sellStock(inputFile, ticker, newPortfolio, day)
        newPortfolio.getStocksMap.remove(ticker)
        val tuple = checkAvailabilityAndBuy(portfolio, newPortfolio, inputFile, stockTickers, day, money)
        if (tuple._2!=0.0) {
          newPortfolio.getStocksMap.put(tuple._1, (day, tuple._2))
        }
        //newPortfolio.printPortfolio(day.toString)
      }

    })

    val total = TimeSeriesUtils.getTotalPortfolioValue(inputFile, day, newPortfolio)

    val readableDay = sdf.format(day)

    portfolioOutput.append((readableDay, total))

    //***LOG.info("Total portfolio value at day "+day+": " + total)
    return buySellPolicy(sim, portfolioOutput, newPortfolio, inputFile, stockTickers, initialMoney, days, currentDayIndex+1)

    //stockTickers.foreach(t => println("day: "+ day+ " stock: "+ t  + " " + getAmountOfStockOnDay(inputFile, day, t)))
  }

  /**
   *
   * This function checks whether a specific ticker is available (data for it is present) in the data set on a specific day and, if so,
   * buys the associated stock.
   *
   * @param portfolio Initial portfolio
   * @param newPortfolio Portfolio being processed
   * @param inputFile Input data Map
   * @param stockTickers List of tickers used by the current set of simulations
   * @param day Current day
   * @param money Amount of money available to buy a new ticker
   * @return
   */
  @scala.annotation.tailrec
  def checkAvailabilityAndBuy(portfolio: Portfolio, newPortfolio: Portfolio, inputFile: Map[(Date, String), Double], stockTickers: List[String], day: Date, money: Double): (String, Double) = {
    val newTicker = PolicyUtilsParallelize.getNewRandomTicker(inputFile, portfolio, stockTickers)
    val amount = PolicyUtilsParallelize.buyStock(inputFile, newTicker, newPortfolio, day, money)
    if (amount!=0.0) {
      return (newTicker, amount)
    }

    checkAvailabilityAndBuy(portfolio, newPortfolio, inputFile, stockTickers, day, money)
  }

  /**
   * This function computes the list of dates present in the given dataset.
   * This is used because going through the dates sequentially wouldn't work as there are missing dates (e.g. weekends) on which no trading is done.
   *
   * @param inputData Input data Map
   * @return List of Dates in the dataset
   */
  //This function retrieves the list of ordered days in the inputData (within the time period specified)
  def getListOfOrderedDays(inputData: RDD[String]) : List[Date] = {
    val daysList = scala.collection.mutable.ListBuffer.empty[Date]

    val days = inputData.map(f => TimeSeriesUtils.getDateFromLine(f)).distinct
    days.collect().sorted.foreach(d => daysList.append(d))

    return daysList.toList
  }

  /**
   * This function is the entry point of our simulation.
   *
   * @param sim Simulation number
   * @param tickers List of chosen tickers for the current set of simulations
   * @param inputData Input data RDD
   * @param days List of days in the current time period
   * @param initialMoney Initial money available for the simulation
   * @return List of Portfolio values for each day in the simulation
   */
  def startSimulation(sim: Int, tickers: List[String], inputData: Map[(Date, String), Double], days: List[Date], initialMoney: Double) : List[(String, Double)] = {

    LOG.info("Starting simulation now!")
    LOG.info("Simulation number: "+sim)

    val portfolioValuesList = scala.collection.mutable.ListBuffer.empty[(String, Double)]

    val emptyPortfolio = new Portfolio

    //days.foreach(day => buySellPolicy(emptyPortfolio, inputData, tickers, day, initialMoney))
    buySellPolicy(sim, portfolioValuesList, emptyPortfolio, inputData, tickers, initialMoney, days, 0)

    LOG.info("Simulation finished")

    //print each action
    //actionsList.foreach(x => println(x))

    return portfolioValuesList.toList
  }

  /**
   * This function computes a random list of stocks from the full list of available tickers.
   * We do this in order to run the simulation on a subset of tickers.
   *
   * @param context SparkContext
   * @param inputFile String representing the input file path
   * @param numberOfStocks Number of random stocks we wish to select from the full list
   * @return RDD Containing the ticker names of the selected stocks
   */
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
    val outputFile: String = configuration.getString("configuration.outputFile")
    val local = configuration.getBoolean("configuration.local")

    LOG.info("Setting up Spark environment..")

    val sparkConf = new SparkConf().setAppName(jobName)

    if (local) {
      sparkConf.setMaster("local")
    }

    val context = new SparkContext(sparkConf)

    /* Generate random start tickers */
    val initialTickers = generateRandomStockList(context, tickerFile, numberOfStocks)
    LOG.info("Random stock portfolio OK")

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

    val outputPath = new Path(outputFile+Constants.SLASH)

    val simsOutput = (context.parallelize(1 to numberOfSims)).flatMap(i => startSimulation(i, initialTickersList, filteredInputList, days, initialMoney).map(el => i+Constants.COMMA+el._1+Constants.COMMA+el._2))

    LOG.info("Deleting output directory..")
    outputPath.getFileSystem(context.hadoopConfiguration).delete(outputPath, true)
    LOG.info("Writing output data..")
    simsOutput.saveAsTextFile(outputFile)

    context.stop()
  }
}
