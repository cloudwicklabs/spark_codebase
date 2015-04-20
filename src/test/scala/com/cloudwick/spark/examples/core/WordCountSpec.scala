package com.cloudwick.spark.examples.core

import com.cloudwick.spark.sparkspec.SparkSpec
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

/**
 * Sample test suite for Spark WordCount application
 */
class WordCountSpec extends FlatSpec with SparkSpec with GivenWhenThen with Matchers {
  "Empty set" should "be counted" in {
    Given("empty set")
    val lines = Array("")

    When("count words")
    val wordCounts = WordCount.count(sc.parallelize(lines), Set()).collect()

    Then("empty count")
    wordCounts shouldBe empty
  }

  "Shakespeare most famous quote" should "be counted" in {
    Given("quote")
    val lines = Array("To be or not to be.", "That is the question.")

    Given("stop words")
    val stopWords = Set("the")

    When("count words")
    val wordCounts = WordCount.count(sc.parallelize(lines), stopWords).collect()

    Then("words counted")
    wordCounts.sortBy(_.word) should equal(Array(
      WordCount("be", 2),
      WordCount("is", 1),
      WordCount("not", 1),
      WordCount("or", 1),
      WordCount("question", 1),
      WordCount("that", 1),
      WordCount("to", 2)))
  }
}
