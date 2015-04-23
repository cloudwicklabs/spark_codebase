package com.cloudwick.cassandra

import com.datastax.driver.core.{Session, Cluster}
import com.typesafe.config.ConfigFactory
import com.websudos.phantom.zookeeper.CassandraManager
import scala.concurrent._

/**
 * Description goes here.
 * @author ashrith
 */
private[cassandra] case object CassandraInitLock

object ConfigurableCassandraManager extends CassandraManager {
  private[this] val config = ConfigFactory.load()
  private[this] var initialized = false
  @volatile private[this] var _session: Session = _

  val nativePort = config.getInt("cassandra.nativePort")

  override def livePort: Int = 9042

  override implicit def session: Session = _session

  override def embeddedPort: Int = nativePort

  override def cluster: Cluster = Cluster.builder()
    .addContactPoint(config.getString("cassandra.host"))
    .withPort(nativePort)
    .withoutJMXReporting()
    .withoutMetrics()
    .build()

  override def initIfNotInited(keySpace: String): Unit = CassandraInitLock.synchronized {
    val strategy = config.getString("cassandra.replication.strategy")
    val factor = config.getInt("cassandra.replication.factor")

    if(!initialized) {
      _session = blocking {
        val s = cluster.connect()
        s.execute(s"CREATE KEYSPACE IF NOT EXISTS $keySpace WITH REPLICATION = {'class': '$strategy', 'replication_factor' : $factor};")
        s.execute(s"USE $keySpace;")
        s
      }
      initialized = true
    }
  }
}
