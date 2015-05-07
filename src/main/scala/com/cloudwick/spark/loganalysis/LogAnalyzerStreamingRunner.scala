package com.cloudwick.spark.loganalysis

import java.io.File
import java.nio.file.{Paths, Files}

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.cloudwick.cassandra.schema.{LocationVisit, LogVolume, StatusCount}
import com.cloudwick.logging.LazyLogging
import com.typesafe.config.{Config, ConfigFactory}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext, Time}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Starts a spark streaming job which does apache httpd log analytics, currently supported analytics:
 *  - Aggregates globally total number of times a status code's (200, 404, 503, ...) have been
 *    encountered
 *  - Aggregates per minute hits received by the web server
 *  - Aggregates counts based on Country & City the request originated from using GeoLocation lookup
 *
 * TODO:
 *  - Add windowed based transformations
 *
 * Running this job locally:
 * 1. Start a local Cassandra instance
 *      `${CASSANDRA_HOME}/bin/cassandra -f`
 * 2. Copy `src/main/resources/reference.conf` to convenient location and edit to match respective
 *    properties to match your current environment
 *      `cp src/main/resources/reference.conf loganalysis.conf`
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
 *       `${GENERATOR_HOME}/bin/generator log --eventsPerSec 1 --outputFormat text \
 *        --destination kinesis --kinesisStreamName logevents --kinesisShardCount 1 \
 *        --awsAccessKey [your_aws_access_key] --awsSecretKey [your_aws_secret_key] \
 *        --awsEndPoint [aws_end_point_url] --loggingLevel debug`
 * 5. Start this streaming job
 *      `${SPARK_HOME}/bin/spark-submit --class com.cloudwick.spark.loganalysis.LogAnalyzerRunner \
 *        --master "local[*]" --files src/main/resources/GeoLite2-City.mmdb \
 *        target/scala-2.10/spark_codebase-assembly-1.0.jar [kafka|kinesis] [config_file]`
 *
 * @author ashrith
 */
object LogAnalyzerStreamingRunner extends LazyLogging {

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

  def main(args: Array[String]) {
    if (args.length < 1) {
      logger.error(
        """
          |Usage: KafkaWordCount <source> <configFile>
          |         source - specifies where to read data from, ex: kinesis, kafka, kafka-direct
          |         configFile - where to read the configurations from (ex: resources/reference.conf)
        """.stripMargin
      )
      System.exit(1)
    }

    val config: Config = args.length match {
      case 2 =>
        val configFile = args(1)
        if (Files.exists(Paths.get(configFile))) {
          ConfigFactory.parseFile(new File(configFile))
        } else {
          logger.warn("Cannot find config file specified at {}. Falling back to default.", args(1))
          ConfigFactory.load("default")
        }
      case _ =>
        ConfigFactory.load("default")
    }

    val kafkaConfig = config.resolve.getConfig("kafka")
    val zkQuorum = kafkaConfig.getString("zookeeper.quorum")
    val brokers = kafkaConfig.getString("brokers")
    val group = kafkaConfig.getString("consumer.group")
    val topics = kafkaConfig.getString("topics")
    val numThreads = kafkaConfig.getInt("threads")
    val kafkaParallelism = kafkaConfig.getInt("parallelism")
    val kinesisConfig = config.resolve.getConfig("kinesis")
    val streamName = kinesisConfig.getString("stream.name")
    val streamAppname = kinesisConfig.getString("app.name")
    val streamInitialPosition = kinesisConfig.getString("initial.position")
    val awsAccessKey = kinesisConfig.getString("aws.access.key")
    val awsSecretKey = kinesisConfig.getString("aws.secret.key")
    val endPointUrl = kinesisConfig.getString("aws.endpoint.url")
    val streamingAppConfig = config.resolve.getConfig("streaming")
    val walEnabled = streamingAppConfig.getString("wal.enabled")
    val batchDuration = streamingAppConfig.getInt("batch.duration.ms")
    var checkpointDir = streamingAppConfig.getString("checkpoint.dir")
    if (checkpointDir.isEmpty) {
      checkpointDir = Files.createTempDirectory(this.getClass.getSimpleName).toString
    }

    /**
     * Creates a Streaming context
     * @return
     */
    def createContext() = {
      println("Creating new Spark Streaming Context...")
      val sparkConf = new SparkConf()
        .setAppName(streamAppname)
        .set("spark.streaming.receiver.writeAheadLog.enable", walEnabled)
      // .set("spark.executor.userClassPathFirst", "true")
      // .set("spark.driver.userClassPathFirst", "true")

      val sc = new SparkContext(sparkConf)
      val ssc = new StreamingContext(sc, Milliseconds(batchDuration))
      var lines: DStream[String] = null

      val defaultStorageLevel = walEnabled match {
        case "true" => StorageLevel.MEMORY_AND_DISK
        case "false" => StorageLevel.MEMORY_AND_DISK_2
      }

      args(0) match {
        case "kafka"|"KAFKA" =>
          /*
           * Kafka receiver based approach
           */
          val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
          val kafkaList = (0 until kafkaParallelism).map { _ =>
            KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, defaultStorageLevel).map(_._2)
          }
          lines = ssc.union(kafkaList)
        case "kafka-direct"|"KAFKA-DIRECT" =>
          /*
           * Using kafka direct api (no receiver-based approach)
           * Features:
           *  - # RDD partitions = # Kafka topic partitions
           *  - No WAL
           *  - Exactly once semantics by using Kafka simple API
           *
           * NOTE: This is a experimental feature introduced in 1.3
           */
          val topicsSet = topics.split(",").toSet
          val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
          val directKafkaStream =
            KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
              ssc, kafkaParams, topicsSet)
          lines = directKafkaStream.map(_._2)
        case "kinesis"|"KINESIS" =>
          /*
           * Kinesis receiver initialization
           */
          val kinesisClient = fromCredentials(awsAccessKey, awsSecretKey, endPointUrl)
          // create multiple receivers based on number of stream shards
          val numShards = kinesisClient.describeStream(streamName).getStreamDescription.getShards.size
          // KinesisUtils uses aws java sdk which requires reading aws creds from system properties
          System.setProperty("aws.accessKeyId", awsAccessKey)
          System.setProperty("aws.secretKey", awsSecretKey)
          val kinesisStreams = (0 until numShards).map { _ =>
            KinesisUtils.createStream(ssc, streamName, endPointUrl, Milliseconds(batchDuration),
              InitialPositionInStream.valueOf(streamInitialPosition), defaultStorageLevel)
          }
          lines = ssc.union(kinesisStreams).map(new String(_))
        case _ =>
          logger.error("Unexpected value found as argument.")
          System.exit(1)
      }

      // create required cassandra schema for storing the results
      LogAnalyzer.createCassandraSchema()

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

      /*
       * Set the checkpoint directory
       */
      println("Checkpoint Dir: " + checkpointDir.toString)
      ssc.checkpoint(checkpointDir)

      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpointDir, createContext)

    ssc.start()
    ssc.awaitTermination()
  }

}
