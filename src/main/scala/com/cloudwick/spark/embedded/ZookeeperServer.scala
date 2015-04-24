package com.cloudwick.spark.embedded

import com.cloudwick.logging.LazyLogging
import org.apache.curator.test.TestingServer

/**
 * Runs an in-memory, "embedded" instance of a ZooKeeper server.
 */
class ZookeeperServer(val port: Int = 2181) extends LazyLogging {
  logger.debug(s"Starting embedded ZooKeeper server on port $port...")

  // Creates a new instance of zookeeper server when an instance of this class is created
  private val server = new TestingServer(port)

  def stop(): Unit ={
    logger.debug(s"Shutting down embedded zookeeper server on port $port...")
    server.close()
    logger.debug(s"Shutdown of zookeeper server on port $port completed")
  }

  // The ZooKeeper connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
  val connectionString: String = server.getConnectString

  // The hostname of the ZooKeeper instance.  Example: `127.0.0.1`
  val hostName: String = connectionString.splitAt(connectionString lastIndexOf ':')._1
}
