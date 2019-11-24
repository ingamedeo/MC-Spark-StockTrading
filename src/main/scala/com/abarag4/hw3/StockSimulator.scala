package com.abarag4.hw3

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Date

import com.abarag4.hw3.GetStockTimeSeries.{configuration, getClass, retrieveTickersTimeSeries}
import com.abarag4.hw3.StockSimulatorParallelize.LOG
import com.abarag4.hw3.models.Portfolio
import com.abarag4.hw3.utils.{PolicyUtils, TickerUtils, TimeSeriesUtils}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object StockSimulator {

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)
  val sdf = new SimpleDateFormat("yyyyMMdd")

  /**
   * This function implements the chosen policy for the simulations; in order to do so, it relies on other utility functions.
   * Please see the comments above the utility functions for more details.
   *
   * @param portfolio Current portfolio object
   * @param inputFile RDD input data
   * @param stockTickers List of tickers used by the current set of simulations
   * @param initialMoney Initial amount of money available
   * @param day Current day
   * @return Resulting portfolio obtained after the policy performs the appropriate actions for that day
   */
  def buySellPolicy(portfolio: Portfolio, inputFile: RDD[((Date, String), (Double, Double))], stockTickers: List[String], initialMoney: Double, day: Date): Portfolio = {

    /*
    * Buy an equal portion of each stock to begin with
     */

    //Perform first action
    if (portfolio.getStocksMap.isEmpty) {
      stockTickers.foreach(t => {
        val moneyToInvestForStock = initialMoney/stockTickers.length
        val amount = PolicyUtils.buyStock(inputFile, t, portfolio, day, moneyToInvestForStock)
        if (amount!=0.0) {
          portfolio.getStocksMap.put(t, (day, amount))
        }
      })

      //portfolio.printPortfolio("initial")

      return portfolio
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
      LOG.debug("ticker: "+ ticker+ " previousPrice: "+previousPrice+ " currentPrice: "+currentPrice)

      if (PolicyUtils.stopLoss(inputFile,ticker, previousPortTuple.get, day, 2)) {
        LOG.debug("stopLoss for ticker "+ticker+" at day "+day.toString)
        val money = PolicyUtils.sellStock(inputFile, ticker, newPortfolio, day)
        newPortfolio.getStocksMap.remove(ticker)
        val tuple = checkAvailabilityAndBuy(portfolio, newPortfolio, inputFile, stockTickers, day, money)
        if (tuple._2!=0.0) {
          newPortfolio.getStocksMap.put(tuple._1, (day, tuple._2))
        }

        //newPortfolio.printPortfolio(day.toString)

        return portfolio

      }

      if (PolicyUtils.gainPlateaued(inputFile, ticker, previousPortTuple.get, day, 0.1)) {
        LOG.debug("gainPlateaued for ticker "+ticker+" at day "+day.toString)
        val money = PolicyUtils.sellStock(inputFile, ticker, newPortfolio, day)
        newPortfolio.getStocksMap.remove(ticker)
        //val newTicker = PolicyUtils.getNewRandomTicker(inputFile, portfolio, stockTickers)
        //val amount = PolicyUtils.buyStock(inputFile, newTicker, newPortfolio, day, money)
        val tuple = checkAvailabilityAndBuy(portfolio, newPortfolio, inputFile, stockTickers, day, money)
        if (tuple._2!=0.0) {
          newPortfolio.getStocksMap.put(tuple._1, (day, tuple._2))
        }
        //newPortfolio.printPortfolio(day.toString)

        return portfolio
      }

    })

    return portfolio
  }

  /**
   *
   * This function checks whether a specific ticker is available (data for it is present) in the data set on a specific day and, if so,
   * buys the associated stock.
   *
   * @param portfolio Initial portfolio
   * @param newPortfolio Portfolio being processed
   * @param inputFile Input data RDD
   * @param stockTickers List of tickers used by the current set of simulations
   * @param day Current day
   * @param money Amount of money available to buy a new ticker
   * @return
   */
  @scala.annotation.tailrec
  def checkAvailabilityAndBuy(portfolio: Portfolio, newPortfolio: Portfolio, inputFile: RDD[((Date, String), (Double, Double))], stockTickers: List[String], day: Date, money: Double): (String, Double) = {
    val newTicker = PolicyUtils.getNewRandomTicker(inputFile, portfolio, stockTickers)
    val amount = PolicyUtils.buyStock(inputFile, newTicker, newPortfolio, day, money)
    if (amount!=0.0) {
      return (newTicker, amount)
    }

    checkAvailabilityAndBuy(portfolio, newPortfolio, inputFile, stockTickers, day, money)
  }

  /**
   * This function computes the list of dates present in the given dataset.
   * This is used because going through the dates sequentially wouldn't work as there are missing dates (e.g. weekends) on which no trading is done.
   *
   * @param inputData Input data RDD
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
   * @param outputFile String containing the output folder in which part* files are saved.
   */
  def startSimulation(sim: Int, tickers: List[String], inputData: RDD[((Date, String), (Double, Double))], days: List[Date], initialMoney: Double, outputFile: String) : Unit = {

    LOG.info("Starting simulation now!")
    //LOG.info("Simulation number: "+sim)

    val emptyPortfolio = new Portfolio

    val portList = scala.collection.mutable.ListBuffer.empty[Portfolio]
    portList.append(emptyPortfolio)

    days.foreach(d => {

      val dayPort = buySellPolicy(portList.head, inputData, tickers, initialMoney, d)
      portList.clear()
      portList.append(dayPort)

      val outputRDD = inputData.filter(el => el._1._1.compareTo(d)==0).filter(el3 => {
        val stockData = dayPort.getStocksMap.get(el3._1._2)
        stockData.nonEmpty
      }).map(el2 => {
        val stockData = dayPort.getStocksMap.get(el2._1._2)
        if (stockData.nonEmpty) {
          (el2._1, (el2._2._1, stockData.get._2))
        } else {
          (el2._1, (el2._2._1, 0))
        }
      })

      outputRDD.saveAsTextFile(outputFile+Constants.SLASH+sim+Constants.SLASH+sdf.format(d))
    })

    LOG.info("Simulation finished")
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
    val outputFile2: String = configuration.getString("configuration.outputFile2")
    val local = configuration.getBoolean("configuration.local")

    LOG.info("Setting up Spark environment..")

    val sparkConf = new SparkConf().setAppName(jobName).setMaster("local")

    if (local) {
      sparkConf.setMaster("local")
    }

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
      ((TimeSeriesUtils.getDateFromLine(x), line(0)), (line(2).toDouble, 0.0))
    }).cache()

    LOG.info("Time series data preparation..")
    val days = getListOfOrderedDays(filteredInput)
    LOG.info("Time series data preparation complete!")

    val initialTickersList = initialTickers.collect.toList

    val outputPath = new Path(outputFile2+Constants.SLASH)

    LOG.info("Deleting output directory..")
    outputPath.getFileSystem(context.hadoopConfiguration).delete(outputPath, true)

    //Due to nested datasets, we can't run this also in parallel
    for (i <- 1 to numberOfSims) {
      startSimulation(i, initialTickersList, inputMap, days, initialMoney, outputFile2)
    }

    context.stop()
  }
}
