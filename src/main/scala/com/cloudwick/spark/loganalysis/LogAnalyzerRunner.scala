package com.cloudwick.spark.loganalysis

import com.cloudwick.logging.LazyLogging
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Spark job to analyze apache http log events
 *  - Aggregates globally total number of times a status code's (200, 404, 503, ...) have been
 *    encountered
 *  - Aggregates per minute hits received by the web server
 *  - Aggregates counts based on Country & City the request originated from using GeoLocation lookup
 *
 * @author ashrith
 */
object LogAnalyzerRunner extends LazyLogging {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: LogAnalyzerRunner input_path output_path")
      System.exit(1)
    }

    val Array(inputPath, outputPath) = args

    val conf = new SparkConf().setAppName("LogAnalyzerRunner")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(inputPath)
    val statusCounts = LogAnalyzer.statusCounter(lines)
    val volumeCounter = LogAnalyzer.volumeCounter(lines)
    val countryCounter = LogAnalyzer.countryCounter(lines)

    statusCounts.saveAsTextFile(outputPath + "/status_counts")
    volumeCounter.saveAsTextFile(outputPath + "/volume_counts")
    countryCounter.saveAsTextFile(outputPath + "/country_counts")
  }
}
