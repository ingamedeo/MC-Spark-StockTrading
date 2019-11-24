package com.abarag4.hw3.utils

import java.io.{BufferedWriter, File, FileWriter}
import java.util.Date

import com.abarag4.hw3.Constants
import com.abarag4.hw3.models.Portfolio
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object TimeSeriesUtils {

  /**
   * This function processes a CSV line and returns some of its columns as a String in a ListBuffer
   *
   * @param fileName Filename of CSV file
   * @param line Current CSV line being processed
   * @param outputList Output list containing CSV lines (Output)
   */
  def processTSLine(fileName: String, line: String, outputList: scala.collection.mutable.ListBuffer[String]) : Unit = {
    val rows = line.split(Constants.COMMA)
    outputList.append(fileName+Constants.COMMA+rows(0)+Constants.COMMA+rows(5)+Constants.COMMA+rows(6))
  }

  /**
   * This function processes the CSV file and returns it as a ListBuffer
   *
   * @param filePath File path of CSV file
   * @param fileContent Content of CSV file
   * @param outputList Output list containing CSV lines (Output)
   */
  def processTimeSeriesFile(filePath: String, fileContent: String, outputList: scala.collection.mutable.ListBuffer[String]): Unit = {

    //clean filename (/blahblahblah/blah/file.csv)
    val fileName = filePath.split(Constants.SLASH).last.stripSuffix(Constants.CSVEXT)

    fileContent.split("\n").drop(1)
      .foreach(a => processTSLine(fileName, a, outputList))
  }

  /**
   * This function takes a tuple (FileName, FileContent) of files and merges them into a single file.
   *
   * @param tuple Input tuple (FileName, FileContent)
   * @param outputFileName File name of output file. This function will take care of writing the file to disk.
   */
  def processTimeSeriesFiles(tuple: RDD[(String, String)], outputFileName: String): Unit = {
    val outputFile = new File(outputFileName)

    val outList = scala.collection.mutable.ListBuffer.empty[String]

    //RDD collected on single machine for list append operation
    tuple.collect().foreach(a => processTimeSeriesFile(a._1, a._2, outList))

    //println(outList.length)

    val buffWriter = new BufferedWriter(new FileWriter(outputFile))
    outList.foreach(entry => buffWriter.write(entry+"\n"))
    buffWriter.close()
  }

  /**
   * Extract date from CSV line.
   *
   * @param line Input CSV line
   * @return Date object
   */
  def getDateFromLine(line: String) : Date = {
    val d = Constants.format.parse(line.split(Constants.COMMA)(1))
    return d
  }

  /**
   * This function checks if the current CSV line contains a certain ticker
   *
   * @param currentLine Input CSV line
   * @param ticker Ticker to be checked
   * @return
   */
  def isMatchingTicker(currentLine: String, ticker: String) : Boolean = {
    val currentTicker = currentLine.split(Constants.COMMA)(0)
    //println(currentTicker)
    return currentTicker.equals(ticker)
  }

  /**
   * This function checkes whether two dates objects are matching
   *
   * @param date1 First input param
   * @param date2 Second input param
   * @return
   */
  def isMatchingDate(date1: Date, date2: Date) : Boolean = {
    val res = date1.compareTo(date2) == 0
    return res
  }

  /**
   *
   * This function checks if the date contained in the given line is after a certain start date
   *
   * @param currentLine Input CSV line
   * @param timePeriodStart Start date
   * @return
   */
  def isAfterStartDate(currentLine: String, timePeriodStart: Date) : Boolean = {

    val currentDate = currentLine.split(Constants.COMMA)(1)

    val currentDateObj = Constants.format.parse(currentDate)
    return currentDateObj.compareTo(timePeriodStart) >= 0
  }

  /**
   *
   * This function checks if the date contained in the given line is after a certain end date
   *
   * @param currentLine Input CSV line
   * @param timePeriodEnd End date
   * @return
   */
  def isbeforeEndDate(currentLine: String, timePeriodEnd: Date) : Boolean = {

    val currentDate = currentLine.split(Constants.COMMA)(1)

    val currentDateObj = Constants.format.parse(currentDate)
    return currentDateObj.compareTo(timePeriodEnd) <= 0
  }

  /**
   *
   * This function generates a filtered RDD starting from the full dataset and given a start and end date.
   *
   * @param context SparkContext
   * @param inputFileName Input file name
   * @param timePeriodStart String indicating the starting date
   * @param timePeriodEnd String indicating the end date
   * @return Filtered RDD that contains only the dates in the time period
   */
  def filterByDate(context: SparkContext, inputFileName: String, timePeriodStart: String, timePeriodEnd: String): RDD[String] = {
    val inputFile = context.textFile(inputFileName)

    val startDate = Constants.format.parse(timePeriodStart)
    val endDate = Constants.format.parse(timePeriodEnd)

    val filteredInputFile = inputFile.filter(line => isAfterStartDate(line, startDate)).filter(line => isbeforeEndDate(line, endDate))
    return filteredInputFile
  }

  /**
   * This function takes in an RDD, a Date and a Ticker and returns the value of that ticker (Stock) on that specific day
   *
   * @param inputFile Input RDD
   * @param currentDay Day
   * @param stockTicker Ticker
   * @return Tuple that contains the value of that ticker on that day and a boolean indicating whether we have data for that ticker on that day
   */
  def getPriceOfStockOnDay(inputFile: RDD[((Date, String), (Double, Double))], currentDay: Date, stockTicker: String) : (Double, Boolean) = {

   // val datesGroup = inputFile.filter(key => (TimeSeriesUtils.isMatchingDate(key._1._1, currentDay)))
    //val amount = datesGroup.filter(el => el._1._2.equals(stockTicker))

    val value = inputFile.lookup((currentDay, stockTicker))

    if (value.nonEmpty) {
      val first = value.head
      return (first._1, true)
    }

    return (0.0, false)
  }

  /**
   * This function takes in a local Map, a Date and a Ticker and returns the value of that ticker (Stock) on that specific day
   *
   * @param inputFile Input Map
   * @param currentDay Day
   * @param stockTicker Ticker
   * @return Tuple that contains the value of that ticker on that day and a boolean indicating whether we have data for that ticker on that day
   */
  def getPriceOfStockOnDay(inputFile: Map[(Date, String), Double], currentDay: Date, stockTicker: String) : (Double, Boolean) = {

    val datesGroup = inputFile.filter(key => (TimeSeriesUtils.isMatchingDate(key._1._1, currentDay)))
    if (datesGroup.nonEmpty) {
      val amount = datesGroup.filter(el => el._1._2.equals(stockTicker))
      if (amount.size == 1) {
        return (amount.head._2, true)
      }
    }

    return (0.0, false)
  }

  /**
   * This function computes the total portfolio value on a specific day
   *
   * @param inputFile Map input file
   * @param currentDay Day for which to compute the total value
   * @param portfolio Current portfolio
   * @return Total portfolio value for that day
   */
  def getTotalPortfolioValue(inputFile: Map[(Date, String), Double], currentDay: Date, portfolio: Portfolio): Double = {

    val total = portfolio.getStocksMap.map(entry => {
      val v = getPriceOfStockOnDay(inputFile, currentDay, entry._1)
      if (v._2) {
        v._1*entry._2._2
      } else {
        0.0
      }
    }).foldLeft(0.0) { (t,i) => t + i }

    return total
  }
}
