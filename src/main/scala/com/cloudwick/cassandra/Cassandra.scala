package com.cloudwick.cassandra

import com.cloudwick.cassandra.schema.{LocationVisitRecord, StatusCountRecord, LogVolumeRecord}
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Description goes here.
 * @author ashrith
 */
trait Cassandra extends CassandraService {
  private[this] val config = ConfigFactory.load()

  protected val cassandraHost = config.getString("cassandra.host")
  protected val nativePort = config.getInt("cassandra.nativePort")
  protected val rpcPort = config.getInt("cassandra.rpcPort")

  def installSchema(): Unit = {
    Await.result(LogVolumeRecord.create.future(), 10.seconds)
    Await.result(StatusCountRecord.create.future(), 10.seconds)
    Await.result(LocationVisitRecord.create.future(), 10.seconds)
  }
}

