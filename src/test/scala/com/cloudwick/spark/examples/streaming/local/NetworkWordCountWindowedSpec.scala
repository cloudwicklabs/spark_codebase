package com.cloudwick.spark.examples.streaming.local

import com.cloudwick.spark.examples.core.WordCount
import com.cloudwick.spark.sparkspec.SparkStreamingSpec
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, Time}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class NetworkWordCountWindowedSpec
  extends FlatSpec
  with SparkStreamingSpec
  with GivenWhenThen
  with Matchers
  with Eventually {

  private val windowDuration = Seconds(4)
  private val slideDuration = Seconds(2)

  // default timeout for eventually trait
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(1500, Millis)))

  "Sample set" should "be counted" in {
    Given("streaming context is initialized")
    val lines = mutable.Queue[RDD[String]]()

    var results = ListBuffer.empty[Array[WordCount]]

    NetworkWordCountWindowed.count(ssc.queueStream(lines), windowDuration, slideDuration, Set()) { (wordsCount: RDD[WordCount], time: Time) =>
      results += wordsCount.collect()
    }

    ssc.start()

    When("first set of words queued")
    lines += sc.makeRDD(Seq("a", "b"))

    Then("words counted after first slide")
    clock.advance(slideDuration.milliseconds)
    eventually {
      results.last should equal(Array(
        WordCount("a", 1),
        WordCount("b", 1)))
    }

    When("second set of words queued")
    lines += sc.makeRDD(Seq("b", "c"))

    Then("words counted after second slide")
    clock.advance(slideDuration.milliseconds)
    eventually {
      results.last should equal(Array(
        WordCount("a", 1),
        WordCount("b", 2),
        WordCount("c", 1)))
    }

    When("nothing more queued")

    Then("word counted after third slide")
    clock.advance(slideDuration.milliseconds)
    eventually {
      results.last should equal(Array(
        WordCount("a", 0),
        WordCount("b", 1),
        WordCount("c", 1)))
    }

    When("nothing more queued")

    Then("word counted after fourth slide")
    clock.advance(slideDuration.milliseconds)
    eventually {
      results.last should equal(Array(
        WordCount("a", 0),
        WordCount("b", 0),
        WordCount("c", 0)))
    }
  }

}
