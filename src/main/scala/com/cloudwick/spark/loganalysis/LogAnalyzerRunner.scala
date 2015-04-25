package com.cloudwick.spark.loganalysis

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.cloudwick.cassandra.schema.{LocationVisit, LogVolume, StatusCount}
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kinesis.KinesisUtils
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
 *      `${CASSANDRA_HOME}/bin/cassandra -f`
 * 2. Edit `src/main/resources/reference.conf` to match respective properties*
 * 3. If using Kafka as data source:
 *    i. Start a zookeeper local instance
 *      `${KAFKA_HOME}/bin/zookeeper-server-start.sh config/zookeeper.properties`
 *    ii. Start a kafka broker local instance
 *      `${KAFKA_HOME}/bin/kafka-server-start.sh config/server.properties`
 *    iii. Create a topic
 *      `${KAFKA_HOME}/bin/kafka-topics.sh --create --zookeeper localhost:2181 \
 *        --replication-factor 1 --partitions 1 --topic log-events`
 *    iv. Make sure the topic got created
 *      `${KAFKA_HOME}/bin/kafka-topics.sh --list --zookeeper localhost:2181`
 *    v. Start the [[https://github.com/cloudwicklabs/generator]] and start sending some messages to
 *       kafka
 *      `${GENERATOR_HOME}/bin/generator log --eventsPerSec 1 --outputFormat text \
 *        --destination kafka --kafkaTopicName log-events --totalEvents 100 --flushBatch 10`
 *    vi. Check the offset consumption of the topic
 *      `${KAFKA_HOME}/bin/kafka-consumer-offset-checker.sh --zookeeper localhost:2181
 *        --topic log-events --group loganalytics`
 * 4. If using kinesis as a data source:
 *    i. Start the [[https://github.com/cloudwicklabs/generator]] and start sending some messages to
 *       kinesis
 *       ${GENERATOR_HOME}/bin/generator log --eventsPerSec 1 --outputFormat text \
 *        --destination kinesis --kinesisStreamName logevents --kinesisShardCount 1 \
 *        --awsAccessKey [your_aws_access_key] --awsSecretKey [your_aws_secret_key] \
 *        --awsEndPoint [aws_end_point_url] --loggingLevel debug`
 * 5. Start this streaming job
 *      `${SPARK_HOME}/spark-submit --class com.cloudwick.spark.loganalysis.LogAnalyzerRunner \
 *        --master "local[*]" --files src/main/resources/GeoLite2-City.mmdb \
 *        target/scala-2.10/spark_codebase-assembly-1.0.jar [kafka|kinesis]`
 *
 * @author ashrith
 */
object LogAnalyzerRunner extends App with Logging {
  if (args.length < 1) {
    log.error(
      """
        |Usage: KafkaWordCount <source>
        |         source - specifies where to read data from, ex: kinesis, kafka
      """.stripMargin
    )
    System.exit(1)
  }

  /**
   * Creates a aws kinesis client connection
   * @param awsAccessKey aws access key
   * @param awsSecretKey aws secret key
   * @param awsEndPoint aws end point url for kinesis
   * @return AmazonKinesisClient connection object
   */
  def fromCredentials(awsAccessKey: String,
                      awsSecretKey: String,
                      awsEndPoint: String): AmazonKinesisClient = {
    val credentials = new AWSCredentials {
      override def getAWSAccessKeyId: String = awsAccessKey

      override def getAWSSecretKey: String = awsSecretKey
    }
    val client = new AmazonKinesisClient(credentials)
    client.setEndpoint(awsEndPoint)
    client
  }

  private[this] val config = ConfigFactory.load()
  val zkQuorum = config.getString("kafka.zookeeper.quorum")
  val group = config.getString("kafka.consumer.group")
  val topics = config.getString("kafka.topics")
  val numThreads = config.getInt("kafka.threads")
  val streamName = config.getString("kinesis.stream.name")
  val streamAppname = config.getString("kinesis.app.name")
  val streamInitialPosition = config.getString("kinesis.initial.position")
  val awsAccessKey = config.getString("kinesis.aws.access.key")
  val awsSecretKey = config.getString("kinesis.aws.secret.key")
  val endPointUrl = config.getString("kinesis.aws.endpoint.url")

  val batchDuration = Seconds(5)

  val sparkConf = new SparkConf()
    .setAppName(streamAppname)
    // .set("spark.executor.userClassPathFirst", "true")
    // .set("spark.driver.userClassPathFirst", "true")

  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, batchDuration)

  var lines: DStream[String] = _

  args(0) match {
    case "kafka"|"KAFKA" =>
      val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
      lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    case "kinesis"|"KINESIS" =>
      val kinesisClient = fromCredentials(awsAccessKey, awsSecretKey, endPointUrl)
      val numShards = kinesisClient.describeStream(streamName).getStreamDescription.getShards.size
      // KinesisUtils uses aws java sdk which requires to read aws credentials from system
      // properties.
      System.setProperty("aws.accessKeyId", awsAccessKey)
      System.setProperty("aws.secretKey", awsSecretKey)
      val kinesisStreams = (0 until numShards).map { i =>
        KinesisUtils.createStream(ssc, streamName, endPointUrl, batchDuration,
          InitialPositionInStream.valueOf(streamInitialPosition), StorageLevel.MEMORY_AND_DISK_2)
      }
      lines = ssc.union(kinesisStreams).map(new String(_))
  }

  // check where guava is loading from.
  // println(this.getClass.getResource("/com/google/common/collect/Sets.class"))

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
