package com.cloudwick.spark.examples.core

import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Simple word count program to illustrate spark standalone applications usage
 */
object WordCountRunner extends App with Logging {
  if (args.length < 2) {
    System.err.println("Usage: WordCountRunner input_path output_path")
    System.exit(1)
  }

  val Array(inputPath, outputPath) = args
  val stopWords = Set("a", "an", "the")

  val conf = new SparkConf().setAppName("WordCount")
  val sc = new SparkContext(conf)

  val lines = sc.textFile(inputPath)
  val counts = WordCount.count(lines, stopWords)

  log.info(counts.collect().mkString("[", ", ", "]"))
}
