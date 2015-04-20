package com.cloudwick.spark.examples.streaming.local

import com.cloudwick.spark.examples.core.WordCount
import com.cloudwick.spark.sparkspec.SparkStreamingSpec
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, Time}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{Matchers, GivenWhenThen, FlatSpec}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class NetworkWordCountSpec
  extends FlatSpec
  with SparkStreamingSpec
  with GivenWhenThen
  with Matchers
  with Eventually {

  private val batch = Seconds(5)

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(1000, Millis)))

  "Sample set" should "be counted" in {
    Given("streaming context is initialized")
    val lines = mutable.Queue[RDD[String]]()

    var results = ListBuffer.empty[Array[WordCount]]

    NetworkWordCount.count(ssc.queueStream(lines), Set()) {
      (wordsCount: RDD[WordCount], time: Time) =>
        results += wordsCount.collect()
    }

    ssc.start()

    When("first batch of words queued")
    lines += sc.makeRDD(Seq("a", "b", "hello", "world", "hello", "b"))

    Then("words counted in batch")
    clock.advance(batch.milliseconds)
    eventually {
      println(results.last)
      results.last should equal(Array(
        WordCount("a", 1),
        WordCount("b", 2),
        WordCount("hello", 2),
        WordCount("world", 1)
      ))
    }
  }
}
