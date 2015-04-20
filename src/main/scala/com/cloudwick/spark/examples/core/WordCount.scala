package com.cloudwick.spark.examples.core

import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Simple word count program to illustrate spark standalone applications usage
 */

case class WordCount(word: String, count: Int)

object WordCount extends Logging {

  def count(lines: RDD[String], stopWords: Set[String]):RDD[WordCount] = {
    val words = prepareWords(lines, stopWords)

    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _).map {
      case (word: String, count: Int) => WordCount(word, count)
    }

    wordCounts
  }

  def prepareWords(lines: RDD[String], stopWords: Set[String]): RDD[String] = {
    lines.flatMap(_.split("\\s+"))
      .map(_.strip(",").strip(".").toLowerCase)
      .filter(!stopWords.contains(_)).filter(!_.isEmpty)
  }
}
