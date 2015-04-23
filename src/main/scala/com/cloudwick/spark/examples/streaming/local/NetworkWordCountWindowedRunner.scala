package com.cloudwick.spark.examples.streaming.local

import java.nio.file.Files
import com.cloudwick.spark.examples.core.WordCount
import com.cloudwick.spark.examples.streaming.local.NetworkWordCountWindowed._
import com.cloudwick.spark.examples.streaming.util.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}

/**
 * Simple example illustrating the Network word count using windowed based operations. This example
 * generates word counts over last 30 seconds of data, every 10 seconds
 *
 * Usage: NetworkWordCountWindowed <hostname> <port>
 *   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive
 *   data
 *
 * To run this start an instance of netcat server:
 *    `nc -lk 9999`
 * and then run this example
 * (NOTE: the master has to be local because we are binding nc to the localhost)
 *    `spark-submit --class com.cloudwick.spark.examples.streaming.local.NetworkWordCountWindowed
 *                  --master local[*] <path_to_jar> localhost 9999`
 */
object NetworkWordCountWindowedRunner extends App with Logging {
  if (args.length < 2) {
    System.err.println("Usage: NetworkWordCount <host> <port>")
    System.exit(1)
  }

  val hostname = args(0)
  val port = args(1).toInt

  private val checkpointDir = Files.createTempDirectory(this.getClass.getSimpleName).toString
  private val windowDuration = Seconds(30)
  private val slideDuration = Seconds(3)
  val stopWords = Set("a", "an", "the")

  log.info(s"Connecting to host: $hostname port: $port")

  Utils.setSparkLogLevels()

  // Create a local StreamingContext with master & specified batch interval
  val conf = new SparkConf().setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(5))

  ssc.checkpoint(checkpointDir)

  // Create a DStream that will connect to host:port
  val lines = ssc.socketTextStream(hostname, port, StorageLevel.MEMORY_AND_DISK_SER)

  NetworkWordCountWindowed.count(lines, windowDuration, slideDuration, stopWords) {
    (wordsCount: RDD[WordCount], time: Time) =>
      val counts = time + ": " + wordsCount.collect().mkString("[", ", ", "]")
      println(counts)
  }

  // Start the computation
  ssc.start()
  // Wait for the computation to terminate
  ssc.awaitTermination()
}
