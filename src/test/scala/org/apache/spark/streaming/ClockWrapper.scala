package org.apache.spark.streaming

import org.apache.spark.util.ManualClock

/**
 * Spark Streaming transformations are depend on batch interval's. We should wait for at-least the
 * time specified for streaming interval in the test suite's to actually see the results. This
 * wrapper class would avoid us to write a lot of `Thread.sleep()` method's in the test suites.
 */
class ClockWrapper(ssc: StreamingContext) {
  private def manualClock: ManualClock = ssc.scheduler.clock.asInstanceOf[ManualClock]

  def getTimeMillis: Long = manualClock.getTimeMillis()

  def setTime(time: Long) = manualClock.setTime(time)

  def advance(timeToAdd: Long) = manualClock.advance(timeToAdd)

  def waitTillTime(targetTime: Long) = manualClock.waitTillTime(targetTime)
}
