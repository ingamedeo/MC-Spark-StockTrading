package com.abarag4.hw3.utils

import com.abarag4.hw3.GetStockTimeSeries.QUOTE

import scala.io.Source

object TickerUtils {

  def getTickerName(line: String) : String = {
    val fields = line.split(",").map(_.trim.stripPrefix(QUOTE).stripSuffix(QUOTE))
    fields(0)
  }

  def createTickerList(tickerList: scala.collection.mutable.ListBuffer[String], fileName: String): Unit = {
    val bufferedSource = Source.fromFile("data/"+fileName)
    bufferedSource.getLines.foreach(l => tickerList.append(getTickerName(l)))
    bufferedSource.close

    //Remove header
    tickerList.remove(0)
  }
}
