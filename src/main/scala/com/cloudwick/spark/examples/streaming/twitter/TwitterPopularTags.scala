package com.cloudwick.spark.examples.streaming.twitter

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, Logging}
import StreamingContext._
// required for sortByKey, which is not exposed in the DStream API
import org.apache.spark.SparkContext._

/**
 * Calculates popular hash-tags (topics) over sliding 10 and 60 seconds windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 * Runing this:
 *  `spark-submit --class com.cloudwick.spark.examples.streaming.twitter.TwitterPopularTags`
 */
object TwitterPopularTags extends App with Logging {
  if (args.length < 4) {
    log.error("Usage: TwitterPopularTags <consumer key> <consumer secret> <access token> " +
      "<access token secret> [<filters>]")
    System.exit(1)
  }

  val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
  val filters = args.takeRight(args.length - 4)

  // Set the system properties so that Twitter4j library used by twitter stream can use them to
  // generate OAuth credentials
  System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
  System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
  System.setProperty("twitter4j.oauth.accessToken", accessToken)
  System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

  val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
  val ssc = new StreamingContext(sparkConf, Seconds(2))
  val stream = TwitterUtils.createStream(ssc, None, filters)

  val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

  val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
    .map{case (topic, count) => (count, topic)}
    .transform(_.sortByKey(ascending = false))

  val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
    .map{case (topic, count) => (count, topic)}
    .transform(_.sortByKey(ascending = false))

  // Print popular hash-tags
  topCounts60.foreachRDD(rdd => {
    val topList = rdd.take(10)
    println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
    topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
  })

  topCounts10.foreachRDD(rdd => {
    val topList = rdd.take(10)
    println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
    topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
  })

  ssc.start()
  ssc.awaitTermination()
}
