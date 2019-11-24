package com.abarag4.hw3

import java.text.SimpleDateFormat
import java.util.Date

import com.abarag4.hw3.StockSimulator.configuration
import com.abarag4.hw3.models.Portfolio
import com.abarag4.hw3.utils.PolicyUtils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.{Logger, LoggerFactory}

class PolicyUtilsTest extends FunSuite with BeforeAndAfter {

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  test("PolicyUtils.sellStock") {

    val ticker: String = configuration.getString("configuration.tests.sellStock.ticker")
    val date1Val: String = configuration.getString("configuration.tests.sellStock.date1")
    val date2Val: String = configuration.getString("configuration.tests.sellStock.date2")
    val stockPrice: Double = configuration.getInt("configuration.tests.sellStock.stockPrice")
    val stockAmount: Double = configuration.getInt("configuration.tests.sellStock.stockAmount")
    val obtainedMoney: Double = configuration.getInt("configuration.tests.sellStock.obtainedMoney")

    val jobName: String = configuration.getString("configuration.jobName")
    val local = configuration.getBoolean("configuration.local")
    val sparkConf = new SparkConf().setAppName(jobName).setMaster("local")

    if (local) {
      sparkConf.setMaster("local")
    }
    val context = new SparkContext(sparkConf)

    val date1 = Constants.format.parse(date1Val)
    val date2 = Constants.format.parse(date2Val)
    val fakeSeq: Seq[((Date, String), (Double, Double))] = Seq(((date2, ticker), (stockPrice, 0)))
    val fakeRDD = context.parallelize(fakeSeq)

    val fakePort1 = new Portfolio
    fakePort1.getStocksMap.put(ticker, (date1, stockAmount))

    val money = PolicyUtils.sellStock(fakeRDD, ticker, fakePort1, date2)
    LOG.info("Money: "+money)

    context.stop()

    assert(money==obtainedMoney)
  }

  test("PolicyUtils.buyStock") {

    val ticker: String = configuration.getString("configuration.tests.buyStock.ticker")
    val date1Val: String = configuration.getString("configuration.tests.buyStock.date1")
    val date2Val: String = configuration.getString("configuration.tests.buyStock.date2")
    val stockPrice: Double = configuration.getInt("configuration.tests.buyStock.stockPrice")
    val stockAmount: Double = configuration.getInt("configuration.tests.buyStock.stockAmount")
    val availableMoney: Double = configuration.getInt("configuration.tests.buyStock.obtainedMoney")

    val jobName: String = configuration.getString("configuration.jobName")
    val local = configuration.getBoolean("configuration.local")
    val sparkConf = new SparkConf().setAppName(jobName).setMaster("local")

    if (local) {
      sparkConf.setMaster("local")
    }
    val context = new SparkContext(sparkConf)

    val date1 = Constants.format.parse(date1Val)
    val date2 = Constants.format.parse(date2Val)
    val fakeSeq: Seq[((Date, String), (Double, Double))] = Seq(((date2, ticker), (stockPrice, 0)))
    val fakeRDD = context.parallelize(fakeSeq)

    val fakePort1 = new Portfolio
    fakePort1.getStocksMap.put(ticker, (date1, stockAmount))

    val fakePort2 = new Portfolio

    //Buy additional 50 units of the stock, while having 50 units already in th portfolio => 50+50=100
    val amount1 = PolicyUtils.buyStock(fakeRDD, ticker, fakePort1, date2, availableMoney)
    LOG.info("Amount (additional): "+amount1)

    //Start with an empty portfolio and buy just 50 units of the stock
    val amount2 = PolicyUtils.buyStock(fakeRDD, ticker, fakePort2, date2, availableMoney)
    LOG.info("Amount: "+amount2)

    context.stop()

    assert(amount1==stockAmount*2)
    assert(amount2==stockAmount)
  }

  test("PolicyUtils.stopLoss") {

    val ticker: String = configuration.getString("configuration.tests.stopLoss.ticker")
    val date1Val: String = configuration.getString("configuration.tests.stopLoss.date1")
    val date2Val: String = configuration.getString("configuration.tests.stopLoss.date2")
    val stockPrice1: Double = configuration.getInt("configuration.tests.stopLoss.stockPrice1")
    val stockPrice2: Double = configuration.getInt("configuration.tests.stopLoss.stockPrice2")
    val stockAmount: Double = configuration.getInt("configuration.tests.stopLoss.stockAmount")
    val delta: Double = configuration.getInt("configuration.tests.stopLoss.delta")
    val isLossTrueVal: Boolean = configuration.getBoolean("configuration.tests.stopLoss.isLossTrue")

    val jobName: String = configuration.getString("configuration.jobName")
    val local = configuration.getBoolean("configuration.local")
    val sparkConf = new SparkConf().setAppName(jobName).setMaster("local")

    if (local) {
      sparkConf.setMaster("local")
    }
    val context = new SparkContext(sparkConf)

    val date1 = Constants.format.parse(date1Val)
    val date2 = Constants.format.parse(date2Val)
    val fakeSeq: Seq[((Date, String), (Double, Double))] = Seq(((date1, ticker), (stockPrice1, 0)), ((date2, ticker), (stockPrice2, 0)))
    val fakeRDD = context.parallelize(fakeSeq)

    val isLossTrue = PolicyUtils.stopLoss(fakeRDD, ticker, (date1, stockAmount), date2, delta)
    LOG.info("isLossTrue: "+isLossTrue)

    if (isLossTrueVal) {
      assert(isLossTrue)
    } else {
      assert(!isLossTrue)
    }

    context.stop()
  }

  test("PolicyUtils.gainPlateaued") {

    val ticker: String = configuration.getString("configuration.tests.gainPlateaued.ticker")
    val date1Val: String = configuration.getString("configuration.tests.gainPlateaued.date1")
    val date2Val: String = configuration.getString("configuration.tests.gainPlateaued.date2")
    val stockPrice1: Double = configuration.getInt("configuration.tests.gainPlateaued.stockPrice1")
    val stockPrice2: Double = configuration.getInt("configuration.tests.gainPlateaued.stockPrice2")
    val stockAmount: Double = configuration.getInt("configuration.tests.gainPlateaued.stockAmount")
    val delta: Double = configuration.getInt("configuration.tests.gainPlateaued.delta")
    val isPlateauTrueVal: Boolean = configuration.getBoolean("configuration.tests.gainPlateaued.isPlateauTrue")

    val jobName: String = configuration.getString("configuration.jobName")
    val local = configuration.getBoolean("configuration.local")
    val sparkConf = new SparkConf().setAppName(jobName).setMaster("local")

    if (local) {
      sparkConf.setMaster("local")
    }
    val context = new SparkContext(sparkConf)

    val date1 = Constants.format.parse(date1Val)
    val date2 = Constants.format.parse(date2Val)
    val fakeSeq: Seq[((Date, String), (Double, Double))] = Seq(((date1, ticker), (stockPrice1, 0)), ((date2, ticker), (stockPrice2, 0)))
    val fakeRDD = context.parallelize(fakeSeq)

    val isPlateauTrue = PolicyUtils.gainPlateaued(fakeRDD, ticker, (date1, stockAmount), date2, delta)
    LOG.info("isPlateauTrue: "+isPlateauTrue)

    if (isPlateauTrueVal) {
      assert(isPlateauTrue)
    } else {
      assert(!isPlateauTrue)
    }

    context.stop()
  }
}
