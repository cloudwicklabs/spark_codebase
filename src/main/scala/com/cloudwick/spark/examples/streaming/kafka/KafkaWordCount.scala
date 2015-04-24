package com.cloudwick.spark.examples.streaming.kafka

import java.nio.file.Files

import com.cloudwick.logging.LazyLogging
import com.cloudwick.spark.examples.core.WordCount
import com.cloudwick.spark.examples.streaming.local.NetworkWordCountWindowed
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.SparkConf

/**
 * Consumes messages from one or more topics in Kafka and does word-count.
 * Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 *
 * Running this example locally:
 * 1. Start a zookeeper local instance
 *      `bin/zookeeper-server-start.sh config/zookeeper.properties`
 * 2. Start a kafka broker local instance
 *      `bin/kafka-server-start.sh config/server.properties`
 * 3. Create a topic
 *      `bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-wc`
 * 4. Make sure the topic got created
 *      `bin/kafka-topics.sh --list --zookeeper localhost:2181`
 * 5. Start console producer and start sending some messages
 *      `bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test-wc`
 * 6. Start this streaming job
 *      `spark-submit --class com.cloudwick.spark.examples.streaming.kafka.KafkaWordCount --master "local[*]" target/scala-2.10/spark_codebase-assembly-1.0.jar localhost:2181 stcg test-wc 1`
 * 7. Check the offset consumption of the topic
 *      `bin/kafka-consumer-offset-checker.sh --zookeeper localhost:2181 --topic test-wc --group stcg`
 */
object KafkaWordCount extends App with LazyLogging {
  if (args.length < 4) {
    logger.error(
      """
        |Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>
        |         zkQuorum - Zookeeper quorum (hostname:port,hostname:port,..)
        |         group - The group id for this consumer
        |         topics - csv of topics to consume
        |         numThreads - number of threads to use for consuming (ideally equal to number of
        |                      partitions)
      """.stripMargin
    )
    System.exit(1)
  }

  val Array(zkQuorum, group, topics, numThreads) = args
  val batchDuration = Seconds(5)
  val windowDuration = Seconds(30)
  val slideDuration = Seconds(10)
  val stopWords = Set("a", "an", "the")
  private val checkpointDir = Files.createTempDirectory(this.getClass.getSimpleName).toString

  val sparkConf = new SparkConf().setAppName("KafkaWordCount")
  val ssc = new StreamingContext(sparkConf, batchDuration)
  ssc.checkpoint(checkpointDir)

  val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
  val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

  NetworkWordCountWindowed.count(lines, windowDuration, slideDuration, stopWords) {
    (wordsCount: RDD[WordCount], time: Time) =>
      val counts = time + ": " + wordsCount.collect().mkString("[", ", ", "]")
      println(counts)
  }

  ssc.start()
  ssc.awaitTermination()
}
