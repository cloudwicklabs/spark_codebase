package com.cloudwick.spark.streaming.local

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream
// required for bringing DStream operators (reduceByKey, ...) to scope not required for Spark 1.3+
import org.apache.spark.streaming.StreamingContext._

// Bring prepareWords() and WordCount case class into scope
import com.cloudwick.spark.examples._

object NetworkWordCount {

  type WordHandler = (RDD[WordCount], Time) => Unit

  def count(lines: DStream[String], stopWords: Set[String])(handler: WordHandler): Unit = {
    val words = lines.transform(WordCount.prepareWords(_, stopWords))

    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _).map {
      case (word: String, count: Int) => WordCount(word, count)
    }

    wordCounts.foreachRDD((rdd: RDD[WordCount], time: Time) => {
      handler(rdd.sortBy(_.word), time)
    })
  }

}
