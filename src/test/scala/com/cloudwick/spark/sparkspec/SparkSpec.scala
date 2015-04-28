package com.cloudwick.spark.sparkspec

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Suite, BeforeAndAfterAll}

/**
 * Extending the Suite for convenience, this base SparkSpec set's the SparkContext app
 */
trait SparkSpec extends BeforeAndAfterAll {
  this: Suite =>

  private val master = "local[2]"
  private val appName = this.getClass.getSimpleName

  private var _sc: SparkContext = _

  def sc = _sc

  val conf: SparkConf = new SparkConf().setAppName(appName).setMaster(master)

  override def beforeAll(): Unit = {
    super.beforeAll()
    _sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    if (_sc != null) {
      _sc.stop()
      _sc = null
    }
    super.afterAll()
  }
}
