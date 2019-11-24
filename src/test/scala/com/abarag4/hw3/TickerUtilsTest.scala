package com.abarag4.hw3

import com.abarag4.hw3.utils.TickerUtils
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.{Logger, LoggerFactory}

class TickerUtilsTest extends FunSuite with BeforeAndAfter {

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  test("TickerUtils.getTickerName") {

    val sampleInputLine = configuration.getString("configuration.tests.sampleInputLine")
    val sampleTicker = configuration.getString("configuration.tests.sampleTicker")

    val ticker = TickerUtils.getTickerName(sampleInputLine)
    LOG.info("sampleTicker: "+sampleTicker)
    LOG.info("ticker: "+ticker)

    assert(ticker.equals(sampleTicker))
  }

}
