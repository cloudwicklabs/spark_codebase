package com.cloudwick.spark.streaming.kafka

import com.cloudwick.spark.streaming.util.Utils
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.streaming.StreamingContext._

/**
 * Consumes messages from one or more topics in Kafka and does word-count.
 * Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 */
object KafkaWordCount extends App with Logging {
  if (args.length < 4) {
    log.error("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
    System.exit(1)
  }

  Utils.setSparkLogLevels()

  val Array(zkQuorum, group, topics, numThreads) = args
  val sparkConf = new SparkConf().setAppName("KafkaWordCount")
  val ssc = new StreamingContext(sparkConf, Seconds(5))
  ssc.checkpoint("checkpoint")

  val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
  val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
  wordCounts.print()

  ssc.start()
  ssc.awaitTermination()
}
