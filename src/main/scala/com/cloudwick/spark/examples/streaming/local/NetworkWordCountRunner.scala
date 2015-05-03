package com.cloudwick.spark.examples.streaming.local

import com.cloudwick.logging.LazyLogging
import com.cloudwick.spark.examples.core.WordCount
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{Logging, SparkConf}

/**
 * Simple example illustrating the Network word count. This basically is a clone from Spark
 * documentation.
 *
 * Usage: NetworkWordCountRunner <hostname> <port> <batchIntervalInSecs>
 *   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive
 *   data
 *
 * To run this start an instance of netcat server:
 *    `nc -lk 9999`
 * and then run this example
 * (NOTE: the master has to be local because we are binding nc to the localhost)
 *    `spark-submit --class com.cloudwick.spark.examples.streaming.local.NetworkWordCountRunner
 *                  --master local[*] <path_to_jar> localhost 9999 5`
 */
object NetworkWordCountRunner extends LazyLogging {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: NetworkWordCount host port batchIntervalInSecs")
      System.exit(1)
    }

    val Array(host, port, batchInterval) = args
    val stopWords = Set("a", "an", "the")

    val conf = new SparkConf().setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

    // Create a DStream that will connect to host:port
    val lines = ssc.socketTextStream(host, port.toInt, StorageLevel.MEMORY_AND_DISK_SER)

    NetworkWordCount.count(lines, stopWords) { (wordsCount: RDD[WordCount], time: Time) =>
      val counts = time + ": " + wordsCount.collect().mkString("[", ", ", "]")
      println(counts)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
