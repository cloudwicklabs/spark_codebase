package com.cloudwick.spark.examples.streaming.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging

object Utils extends Logging {
  /**
   * Set reasonable logging levels for Spark Streaming if the user has not configured log4j
   */
  def setSparkLogLevels(): Unit = {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the logging
      // level
      logInfo("Setting log level to [WARN] for spark streaming examples. " +
        "To override add a custom log4j.properties to the classpath")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
