package com.cloudwick.spark.embedded

import java.nio.file.Files
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, KafkaServerStartable}
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.Logging

import scala.concurrent.duration._

/**
 * Runs an in-memory, "embedded" instance of a Kafka broker, which listens at `127.0.0.1:9092`
 * by default.
 *
 * Requires a running ZooKeeper instance to connect to.  By default, it expects a ZooKeeper instance
 * running at `127.0.0.1:2181`.
 *
 * @param config Broker configuration settings.  Used to modify, for example, on which port the
 *               broker should listen to.
 */
class KafkaServer(config: Properties = new Properties) extends Logging {
  private val defaultZkConnect = "127.0.0.1:2181"
  private val logDir = Files.createTempDirectory(this.getClass.getSimpleName)

  private val effectiveConfig = {
    val c = new Properties
    c.load(this.getClass.getResourceAsStream("/broker-defaults.properties"))
    c.putAll(config)
    c.setProperty("log.dirs", logDir.toString)
    c
  }

  private val kafkaConfig = new KafkaConfig(effectiveConfig)
  private val kafka = new KafkaServerStartable(kafkaConfig)

  // This is broker's `metadata.broker.list` value.  Example: `127.0.0.1:9092`.
  val brokerList = kafka.serverConfig.hostName + ":" + kafka.serverConfig.port

  // The ZooKeeper connection string aka `zookeeper.connect`.
  val zookeeperConnect = {
    val zkConnectLookup = Option(effectiveConfig.getProperty("zookeeper.connect"))
    zkConnectLookup match {
      case Some(zkConnect) => zkConnect
      case _ =>
        log.warn(s"zookeeper.connect is not configured -- falling back to default " +
          s"setting $defaultZkConnect")
        defaultZkConnect
    }
  }

  // Start the broker
  def start() {
    log.debug(s"Starting embedded Kafka broker at $brokerList (with ZK server " +
      s"at $zookeeperConnect) ...")
    kafka.startup()
    log.debug(s"Startup of embedded Kafka broker at $brokerList completed (with ZK server " +
      s"at $zookeeperConnect)")
  }

  // Stop the broker
  def stop() {
    log.debug(s"Shutting down embedded Kafka broker at $brokerList (with ZK server " +
      s"at $zookeeperConnect)...")
    kafka.shutdown()
    Files.deleteIfExists(logDir)
    log.debug(s"Shutdown of embedded Kafka broker at $brokerList completed (with ZK server " +
      s"at $zookeeperConnect)")
  }

  def createTopic(topic: String,
                  partitions: Int = 1,
                  replicationFactor: Int = 1,
                  config: Properties = new Properties): Unit = {
    log.debug(s"Creating topic { name: $topic, partitions: $partitions, " +
      s"replicationFactor: $replicationFactor, config: $config }")
    val sessionTimeout = 10.seconds
    val connectionTimeout = 8.seconds
    // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
    // createTopic() will only seem to work (it will return without error).  Topic will exist in
    // only ZooKeeper, and will be returned when listing topics, but Kafka itself does not create
    // the topic.
    val zkClient = new ZkClient(zookeeperConnect,
      sessionTimeout.toMillis.toInt,
      connectionTimeout.toMillis.toInt,
      ZKStringSerializer
    )
    AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor, config)
    zkClient.close()
  }
}
