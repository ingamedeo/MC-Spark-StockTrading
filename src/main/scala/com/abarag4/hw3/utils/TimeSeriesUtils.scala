package com.abarag4.hw3.utils

import java.io.{BufferedWriter, File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Date

import com.abarag4.hw3.Constants
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object TimeSeriesUtils {

  def processTSLine(fileName: String, line: String, outputList: scala.collection.mutable.ListBuffer[String]) : Unit = {
    val rows = line.split(Constants.COMMA)
    outputList.append(fileName+Constants.COMMA+rows(0)+Constants.COMMA+rows(5)+Constants.COMMA+rows(6))
  }

  def processTimeSeriesFile(filePath: String, fileContent: String, outputList: scala.collection.mutable.ListBuffer[String]): Unit = {

    //clean filename
    val fileName = filePath.split(Constants.SLASH).last.stripSuffix(Constants.CSVEXT)

    fileContent.split("\n").drop(1)
      .foreach(a => processTSLine(fileName, a, outputList))
  }

  def processTimeSeriesFiles(tuple: RDD[(String, String)], outputFileName: String): Unit = {
    val outputFile = new File(outputFileName)

    val outList = scala.collection.mutable.ListBuffer.empty[String]

    //RDD collected on single machine for list append operation
    tuple.collect().foreach(a => processTimeSeriesFile(a._1, a._2, outList))

    println(outList.length)

    val buffWriter = new BufferedWriter(new FileWriter(outputFile))
    outList.foreach(entry => buffWriter.write(entry+"\n"))
    buffWriter.close()
  }

  def getDateFromLine(line: String) : Date = {
    val d = Constants.format.parse(line.split(Constants.COMMA)(1))
    return d
  }

  def isMatchingTicker(currentLine: String, ticker: String) : Boolean = {
    val currentTicker = currentLine.split(Constants.COMMA)(0)
    //println(currentTicker)
    return currentTicker.equals(ticker)
  }

  def isMatchingDate(date1: Date, date2: Date) : Boolean = {
    val res = date1.compareTo(date2) == 0
    return res
  }

  def isAfterStartDate(currentLine: String, timePeriodStart: Date) : Boolean = {

    val currentDate = currentLine.split(Constants.COMMA)(1)

    val currentDateObj = Constants.format.parse(currentDate)
    return currentDateObj.compareTo(timePeriodStart) >= 0
  }

  def isbeforeEndDate(currentLine: String, timePeriodEnd: Date) : Boolean = {

    val currentDate = currentLine.split(Constants.COMMA)(1)

    val currentDateObj = Constants.format.parse(currentDate)
    return currentDateObj.compareTo(timePeriodEnd) <= 0
  }

  def filterByDate(context: SparkContext, inputFileName: String, timePeriodStart: String, timePeriodEnd: String): RDD[String] = {
    val inputFile = context.textFile(inputFileName)

    val startDate = Constants.format.parse(timePeriodStart)
    val endDate = Constants.format.parse(timePeriodEnd)

    val filteredInputFile = inputFile.filter(line => isAfterStartDate(line, startDate)).filter(line => isbeforeEndDate(line, endDate))
    return filteredInputFile
  }
}
