package com.cloudwick.spark.examples.streaming.kafka

import java.nio.file.Files
import java.util.concurrent.{TimeUnit, Executors}

import com.cloudwick.logging.LazyLogging
import com.cloudwick.spark.loganalysis.LogEvent
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.format.DateTimeFormat

import scala.concurrent.{Future, ExecutionContext}

/**
 * Uses KafkaDirectAPI and Stateful transformation `updateStateByKey` to count the number of times
 * a user IP has been seen, only keeping last 10 mins data
 * @author ashrith
 */
object StatefulKafkaWordCount extends LazyLogging {

  private val logEventPattern = """([\d.]+) (\S+) (\S+) \[(.*)\] "([^\s]+) (/[^\s]*) HTTP/[^\s]+" (\d{3}) (\d+) "([^"]+)" "([^"]+)"""".r
  private val formatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z")
  private val KEY_REMOVE_TIME_MS = 60 * 1000

  /*
  val executionContext = Executors.newScheduledThreadPool(1)

  def cleanUpState(state: DStream[(String, (Long, Long))]): Runnable = new Runnable {
    override def run(): Unit = {
      state.map[Option[(String, (Long, Long))]](record => {
        if (System.currentTimeMillis() - record._2._1 > KEY_REMOVE_TIME_MS) {
          println("Cleaning up record: " + record._1)
          None
        } else {
          Some(record)
        }
      }).flatMap(x => x)
    }
  }
  */

  /**
   * Parses the raw log event and generates a `LogEvent` object
   * @param le raw log event
   * @return
   */
  def parseLogEvent(le: String): Option[LogEvent] = {
    le match {
      case logEventPattern(ip, ci, ui, ts, rt, rp, rc, rs, r, ua) =>
        Some(LogEvent(ip, ci, ui, formatter.parseDateTime(ts), rt, rp, rc.toInt, rs.toInt, r, ua))
      case _ => None
    }
  }

  /**
   * Update function passed to updateStateByKey which inturn calls updateIpCounter, which might
   * returns Option of key to keep
   * @param iterator of tuples (Key, Seq(Values), Option[Output])
   */
  def newUpdateFunction(iterator: Iterator[(String, Seq[(Long, Long)], Option[(Long, Long)])]) = {
    iterator.flatMap { t => updateIpCounter(t._2, t._3).map(s => (t._1, s)) }
  }

  /**
   * This function is called for to union of keys in the Reduce DStream with the active batch
   * events with ip address being the key. This produces a stateful RDD that has all the active
   * ip address and their counts which are not more than one day old
   *
   * @param values sequence of (maxTimeStamp, countOfEvents)
   * @param state existing state of the key (maxTimeStamp, countOfEvents)
   * @return new state with values for the current batch considered
   */
  def updateIpCounter(values: Seq[(Long, Long)], state: Option[(Long, Long)]): Option[(Long, Long)] = {
    // return value from the function, to delete a key from the state just return `None`
    var result: Option[(Long, Long)] = null

    // If the current batch has no values associated to a key, then check for the existing state
    // if any of the keys are expiring
    if (values.size == 0) {
      if (System.currentTimeMillis() - state.get._1 > KEY_REMOVE_TIME_MS) {
         logger.warn("Clearing key - inside no value block")
         result = None
      } else {
        result = Some((state.get._1, state.get._2))
      }
    } else {
      // As we applied reduce function previously we are only ever going to get at most one event
      // in the Sequence
      values.foreach(event => {
        // if the state is empty, that means this is the first time are are encountering an ip address
        if (state.isEmpty) {
          result = Some(event)
        } else {
          if (System.currentTimeMillis() - state.get._1 > KEY_REMOVE_TIME_MS) {
             logger.warn("Clearing key - inside values existing block")
             result = None
          } else {
            // state is non empty
            // println("current state: " + state.get)
            result = Some((
              Math.max(event._1, state.get._1),
              event._2 + state.get._2
            ))
          }
        }
      })
    }

    result
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      logger.error(
        """
          |Usage: StatefulKafkaWordCount <zkQuorum> <group> <topics> <numThreads>
          |         brokers - List of kafka brokers (hostname:port,hostname:port,..)
          |         topics - csv of topics to consume
        """.stripMargin
      )
      System.exit(1)
    }

    val Array(brokers, topics) = args
    val batchDuration = Seconds(5)
    val stopWords = Set("a", "an", "the")
    val checkpointDir = Files.createTempDirectory(this.getClass.getSimpleName).toString

    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, batchDuration)
    ssc.checkpoint(checkpointDir)

    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    // Get the kafka stream
    val directKafkaStream =
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topicSet)
    val lines = directKafkaStream.map(_._2)

    // parse the log events and construct LogEvent objects
    val parsedEvents = lines.flatMap(_.split("\\n")).flatMap(parseLogEvent)

    // take the parsed log events and build a tuple's of `(ipAddress, (eventGeneratedTimeStamp, LogEvent))`
    val reqTuples = parsedEvents.map[(String, (Long, LogEvent))](logEvent => {
      val timeInMilliSecs = logEvent.timeStamp.getMillis

      (logEvent.ip, (timeInMilliSecs, logEvent))
    })

    // takes in the constructed tuple's and transform it to the reducedBy tuple's including count's
    val latestBatch = reqTuples.map[(String, (Long, Long))](t => {
      // (ip, (timestamp, counter))
      (t._1, (t._2._1, 1))
    }).reduceByKey((a, b) => {
      // transform to (ip, (timestamp, sumOfCounter))
      (Math.max(a._1, b._1), a._2 + b._2)
    })

    // store and update the state of the key using `updateStateByKey`
    val latestInfo = latestBatch.updateStateByKey[(Long, Long)](
      newUpdateFunction _,
      new HashPartitioner(ssc.sparkContext.defaultParallelism),
      true
    )

    latestInfo.print()

    // schedule the task to run at every 1 minute interval
    // executionContext.scheduleAtFixedRate(cleanUpState(latestInfo), 0, 1, TimeUnit.MINUTES)

    ssc.start()
    ssc.awaitTermination()
  }
}
