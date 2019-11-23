package com.abarag4.hw3.utils

import com.abarag4.hw3.GetStockTimeSeries.QUOTE

import scala.io.Source

object TickerUtils {

  /**
   * Get ticker name from CSV line
   *
   * @param line Input line from CSV file
   * @return Ticker name
   */
  def getTickerName(line: String) : String = {
    val fields = line.split(",").map(_.trim.stripPrefix(QUOTE).stripSuffix(QUOTE))
    fields(0)
  }

  /**
   * Create a list of tickers from the input file. (Check SP500.csv for input file format)
   *
   * @param tickerList List of tickers (Output)
   * @param fileName Input file name
   */
  def createTickerList(tickerList: scala.collection.mutable.ListBuffer[String], fileName: String): Unit = {
    val bufferedSource = Source.fromFile("data/"+fileName)
    bufferedSource.getLines.foreach(l => tickerList.append(getTickerName(l)))
    bufferedSource.close

    //Remove header
    tickerList.remove(0)
  }
}
