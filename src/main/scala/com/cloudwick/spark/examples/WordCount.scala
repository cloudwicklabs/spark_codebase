package com.cloudwick.spark.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.SparkContext._

/**
 * Simple word count program to illustrate spark standalone applications usage
 *
 * @author ashrith
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
