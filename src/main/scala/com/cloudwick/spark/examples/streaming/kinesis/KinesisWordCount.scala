package com.cloudwick.spark.examples.streaming.kinesis

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.cloudwick.logging.LazyLogging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

/**
 * Kinesis word count example
 *
 * Running this example locally:
 *
 *  `spark-submit --class com.cloudwick.spark.examples.streaming.kinesis.KinesisWordCount
 *    --master "local[*]" target/scala-2.10/spark_codebase-assembly-1.0.jar
 *    <stream-name> <aws-access-key> <aws-secret-key> <endpoint-url>`
 */
object KinesisWordCount extends App with LazyLogging {
  if (args.length < 4) {
    logger.error(
      """
        |Usage: KinesisWordCount <stream-name> <aws-access-key> <aws-secret-key> <endpoint-url>
        |         stream-name - is the name of the kinesis stream
        |         aws-access-key - is the aws access key
        |         aws-secret-key - is the aws secret access keuy
        |         endpoint-url - is the endpoint of the kinesis service
      """.stripMargin
    )
    System.exit(1)
  }

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

  val Array(streamName, awsAccessKey, awsSecretKey, endPointUrl) = args

  // Determine the number of shards for a specified stream, so that we could create one kinesis
  // receiver for each shard
  val kinesisClient = fromCredentials(awsAccessKey, awsSecretKey, endPointUrl)
  val numShards = kinesisClient.describeStream(streamName).getStreamDescription.getShards.size

  val batchDuration = Seconds(2)
  val sparkConf = new SparkConf().setAppName("KinesisWordCount").setMaster("local[*]")
  val ssc = new StreamingContext(sparkConf, batchDuration)

  // create receivers
  // set aws.accessKeyId and aws.secretKey as system properties
  System.setProperty("aws.accessKeyId", awsAccessKey)
  System.setProperty("aws.secretKey", awsSecretKey)
  val kinesisStreams = (0 until numShards).map { i =>
    KinesisUtils.createStream(ssc, streamName, endPointUrl, batchDuration,
      InitialPositionInStream.TRIM_HORIZON, StorageLevel.MEMORY_AND_DISK_2)
  }

  // union all the streams
  val unionStream = ssc.union(kinesisStreams)

  // convert each record of type Byte to string
  val words = unionStream.flatMap(new String(_).split("\\s+"))
  val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)

  wordCounts.print()

  ssc.start()
  ssc.awaitTermination()
}
