package com.cloudwick.spark.sparkspec

import org.apache.spark.sql.SQLContext
import org.scalatest.Suite

trait SparkSqlSpec extends SparkSpec {
  this: Suite =>

  private var _sqlc: SQLContext = _

  def sqlc = _sqlc

  override def beforeAll(): Unit = {
    super.beforeAll()

    _sqlc = new SQLContext(sc)
  }

  override def afterAll(): Unit = {
    _sqlc = null

    super.afterAll()
  }
}
