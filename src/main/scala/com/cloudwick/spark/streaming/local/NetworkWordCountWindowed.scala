package com.cloudwick.spark.streaming.local

import com.cloudwick.spark.streaming.util.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Time, Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.streaming.StreamingContext._

// Bring prepareWords() and WordCount case class into scope
import com.cloudwick.spark.examples._


object NetworkWordCountWindowed extends App with Logging {

  type WordHandler = (RDD[WordCount], Time) => Unit

  def count(lines: DStream[String], windowDuration: Duration, slideDuration: Duration, stopWords: Set[String])(handler: WordHandler): Unit = {
    val words = lines.transform(WordCount.prepareWords(_, stopWords))

    val wordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, windowDuration, slideDuration).map {
      case (word: String, count: Int) => WordCount(word, count)
    }

    wordCounts.foreachRDD((rdd: RDD[WordCount], time: Time) => {
      handler(rdd.sortBy(_.word), time)
    })
  }

}
