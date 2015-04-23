package com.cloudwick.cassandra.service

import com.cloudwick.cassandra.schema.LogVolume
import com.datastax.driver.core.ResultSet

import scala.concurrent.Future

/**
 * Description goes here.
 * @author ashrith
 */
trait LogVolumeServiceModule {
  def logVolumeService: LogVolumeService

  trait LogVolumeService {
    def update(logVolume: LogVolume): Future[ResultSet]
    def getCount(id: Long): Future[Option[Long]]
  }
}
