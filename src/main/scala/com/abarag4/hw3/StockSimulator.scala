package com.abarag4.hw3

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.Date

import com.abarag4.hw3.GetStockTimeSeries.{configuration, getClass, retrieveTickersTimeSeries}
import com.abarag4.hw3.utils.{TickerUtils, TimeSeriesUtils}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object StockSimulator {

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  @SerialVersionUID(100L)
  class Portfolio extends Serializable {

    //Key: Ticker Value: (StockPrice, StockAmount)
    val stocksMap = scala.collection.mutable.Map.empty[String, (Double, Double)]

    def getStocksMap: scala.collection.mutable.Map[String, (Double, Double)] = {
      return stocksMap
    }

    def printPortfolio(des: String): Unit = {

      LOG.info("*** Portfolio "+des+ " ***")
      stocksMap.foreach(stock => {
        LOG.info("Stock: "+stock._1+" priceBought: "+stock._2._1+ " stockAmount: "+stock._2._2)
      })
      LOG.info("*** END Portfolio "+des+ " ***")

    }
  }

  def getPriceOfStockOnDay(inputFile: Map[(Date, String), Double], currentDay: Date, stockTicker: String) : (Double, Boolean) = {

    //.filter(el => TimeSeriesUtils.isMatchingTicker(el._2, stockTicker))
    val datesGroup = inputFile.filter(key => (TimeSeriesUtils.isMatchingDate(key._1._1, currentDay)))
    if (datesGroup.nonEmpty) {
      val amount = datesGroup.filter(el => el._1._2.equals(stockTicker))
      if (amount.size == 1) {
        return (amount.head._2, true)
      }
    }

    return (0.0, false)
  }

  def buySellPolicy(actionList: scala.collection.mutable.ListBuffer[Portfolio], inputFile: Map[(Date, String), Double], stockTickers: List[String], day: Date, initialMoney: Double): Unit = {

    //Perform first action
    if (actionList.isEmpty) {
      val initialPortfolio = new Portfolio
      stockTickers.foreach(t => {
        val currentVal = getPriceOfStockOnDay(inputFile, day, t)
        initialPortfolio.getStocksMap.put(t, (currentVal._1, initialMoney/stockTickers.length))
      })

      actionList.append(initialPortfolio)

      initialPortfolio.printPortfolio("initial "+day)

      return
    }

    //Retrieve last portfolio
    val lastPortfolio = actionList.last

    //Print
    lastPortfolio.printPortfolio("last")

    val newPortfolio: Portfolio = new Portfolio

    //Copy elements between portfolios
    lastPortfolio.getStocksMap.foreach(s => newPortfolio.getStocksMap.put(s._1, s._2))

    stockTickers.foreach(ticker => {

      //Get (StockPrice, AmountStock) at previous day
      val previousPortTuple = lastPortfolio.getStocksMap.get(ticker)

      //LOG.debug("previousPort: "+previousPortTuple+ " actionList: "+actionList.length)

      //This ticker had  already been bought
      if (previousPortTuple.nonEmpty) {
        val previousPrice = previousPortTuple.get._1
        val previousAmount = previousPortTuple.get._2

        val currentPrice = getPriceOfStockOnDay(inputFile, day, ticker)._1

        LOG.debug("previousPrice: "+previousPrice+ " currentPrice: "+currentPrice)

        //Stock value decreasing -> Sell
        if (previousPrice > currentPrice) {
          val newAmount = previousAmount - previousAmount*0.10
          newPortfolio.getStocksMap.put(ticker, (currentPrice, newAmount))
          LOG.debug("["+day+"] [Sell] ["+ticker+"] previousPrice: "+previousPrice+ " currentPrice: "+currentPrice)
        } else if (previousPrice < currentPrice) { //Stock value increasing -> Buy more
          val newAmount = previousAmount + previousAmount*0.10
          newPortfolio.getStocksMap.put(ticker, (currentPrice, newAmount))
          LOG.debug("["+day+"] [Buy] ["+ticker+"] previousPrice: "+previousPrice+ " currentPrice: "+currentPrice)
        }

      } else {
        LOG.info("*** Adding ticker ["+ticker+ "] ***")
      }

      actionList.append(newPortfolio)

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

    val actionsList = scala.collection.mutable.ListBuffer.empty[Portfolio]

    days.foreach(day => buySellPolicy(actionsList, inputData, tickers, day, initialMoney))

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
    })

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
