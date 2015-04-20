package com.cloudwick.spark.examples.streaming.local

import com.cloudwick.spark.examples.core.WordCount
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Time}

// Bring prepareWords() and WordCount case class into scope
import com.cloudwick.spark.examples._


object NetworkWordCountWindowed {

  type WordHandler = (RDD[WordCount], Time) => Unit

  def count(lines: DStream[String],
            windowDuration: Duration,
            slideDuration: Duration,
            stopWords: Set[String])
           (handler: WordHandler): Unit = {
    val words = lines.transform(WordCount.prepareWords(_, stopWords))

    val wordCounts =
      words
        .map(x => (x, 1))
        .reduceByKeyAndWindow(_ + _, _ - _, windowDuration, slideDuration)
        .map {
          case (word: String, count: Int) => WordCount(word, count)
        }

    wordCounts.foreachRDD((rdd: RDD[WordCount], time: Time) => {
      handler(rdd.sortBy(_.word), time)
    })
  }

}
