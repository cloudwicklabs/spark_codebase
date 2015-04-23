package com.cloudwick.cassandra

import com.typesafe.config.ConfigFactory
import com.websudos.phantom.zookeeper.{CassandraManager, SimpleCassandraConnector}

/**
 * Description goes here.
 * @author ashrith
 */
trait CassandraService extends SimpleCassandraConnector with CassandraExecutionContext {
   override def keySpace: String = {
      val config = ConfigFactory.load()
      config.getString("cassandra.keyspace")
   }

   override def manager: CassandraManager = ConfigurableCassandraManager
}
