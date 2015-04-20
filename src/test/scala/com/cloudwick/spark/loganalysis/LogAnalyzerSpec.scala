package com.cloudwick.spark.loganalysis

import org.scalatest.{Matchers, FlatSpec}

/**
 * Description goes here.
 * @author ashrith
 */
class LogAnalyzerSpec extends FlatSpec with Matchers {
  "Parser with proper log event" should "return back not None" in {
    val sampleLogEvent = "95.22.50.11 - - [09/Sep/2013:16:36:44 -0700] \"GET /test.php HTTP/1.1\" 200 1832 \"-\" \"Mozilla/5.0 (X11; Linux x86_64; rv:6.0a1) Gecko/20110421 Firefox/6.0a1\""
    //println(LogAnalyzer.parseLogEvent(sampleLogEvent))
    assert(LogAnalyzer.parseLogEvent(sampleLogEvent) != None)
  }

  "Parser with improper log event" should "return back None" in {
    val sampleLogEvent = "[09/Sep/2013:16:36:44 -0700] \"GET /test.php HTTP/1.1\" 200 1832 \"-\" \"Mozilla/5.0 (X11; Linux x86_64; rv:6.0a1) Gecko/20110421 Firefox/6.0a1\""
    assert(LogAnalyzer.parseLogEvent(sampleLogEvent) == None)
  }
}
