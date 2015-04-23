package com.cloudwick.spark.loganalysis

import com.cloudwick.cassandra.schema.{LocationVisit, LogVolume, StatusCount}
import com.cloudwick.spark.examples.streaming.util.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Starts a spark streaming job which does apache httpd log analytics, currently supported analytics:
 *  - Aggregates globally total number of times a status code's (200, 404, 503, ...) have been
 *    encountered
 *  - Aggregates per minute hits received by the web server
 *
 * TODO:
 *  - Add CheckPointing
 *  - Add windowed based transformations
 *  - Multiple kafka receivers for cluster deployments
 *
 * Running this job locally:
 * 1. Start a local Cassandra instance
 *      `bin/cassandra -f`
 * 2. Start a zookeeper local instance
 *      `bin/zookeeper-server-start.sh config/zookeeper.properties`
 * 3. Start a kafka broker local instance
 *      `bin/kafka-server-start.sh config/server.properties`
 * 4. Create a topic
 *      `bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 \
 *        --partitions 1 --topic log-events`
 * 5. Make sure the topic got created
 *      `bin/kafka-topics.sh --list --zookeeper localhost:2181`
 * 6. Start the [[https://github.com/cloudwicklabs/generator]] and start sending some messages to
 *    kafka
 *      `bin/generator log --eventsPerSec 1 --outputFormat text --destination kafka \
 *        --kafkaTopicName log-events --totalEvents 100 --flushBatch 10`
 * 7. Start this streaming job
 *      `spark-submit --class com.cloudwick.spark.loganalysis.LogAnalyzerRunner --master "local[*]"\
 *        --files src/main/resources/GeoLite2-City.mmdb \
 *        target/scala-2.10/spark_codebase-assembly-1.0.jar \
 *        localhost:2181 loganalytics log-events 1`
 * 8. Check the offset consumption of the topic
 *      `bin/kafka-consumer-offset-checker.sh --zookeeper localhost:2181 --topic log-events \
 *        --group loganalytics`
 *
 * @author ashrith
 */
object LogAnalyzerRunner extends App with Logging {
  if (args.length < 4) {
    log.error(
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

  val batchDuration = Seconds(5)

  val Array(zkQuorum, group, topics, numThreads) = args

  Utils.setSparkLogLevels()

  val sparkConf = new SparkConf()
    .setAppName("LogAnalytics")
    // .set("spark.executor.userClassPathFirst", "true")
    // .set("spark.driver.userClassPathFirst", "true")

  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, batchDuration)

  val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
  val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

  println(this.getClass.getResource("/com/google/common/collect/Sets.class"))

  LogAnalyzer.statusCounter(lines) {(statusCount: RDD[StatusCount], time: Time) =>
    val statusCounts = statusCount.collect()
    println("StatusCounter: " + time + ": " + statusCounts.mkString("[", ", ", "]"))
  }

  LogAnalyzer.volumeCounter(lines) {(volumeCount: RDD[LogVolume], time: Time) =>
    val counts = "VolumeCounter: " + time + ": " + volumeCount.collect().mkString("[", ", ", "]")
    println(counts)
  }

  LogAnalyzer.countryCounter(lines) {(countryCount: RDD[LocationVisit], time: Time) =>
    val counts = "CountryCounts: " + time + ": " + countryCount.collect().mkString("[", ", ", "]")
    println(counts)
  }

  ssc.start()
  ssc.awaitTermination()
}
