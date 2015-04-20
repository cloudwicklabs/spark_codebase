package com.cloudwick.spark.sparkspec

import java.nio.file.Files
import org.apache.spark.streaming.{Seconds, ClockWrapper, StreamingContext}
import org.scalatest.Suite

trait SparkStreamingSpec extends SparkSpec {
  this: Suite =>

  private var _ssc: StreamingContext = _
  private var _clock: ClockWrapper = _
  val batchDuration = Seconds(1)
  val checkpointDir = Files.createTempDirectory(this.getClass.getSimpleName)

  def ssc = _ssc
  def clock = _clock

  conf.set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")

  override def beforeAll(): Unit = {
    super.beforeAll()

    _ssc = new StreamingContext(sc, batchDuration)
    _ssc.checkpoint(checkpointDir.toString)

    _clock = new ClockWrapper(ssc)
  }

  override def afterAll(): Unit = {
    if (_ssc != null) {
      _ssc.stop(stopSparkContext = false, stopGracefully = false)
      _ssc = null
    }

    super.afterAll()
  }
}
