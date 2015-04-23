package com.cloudwick.cassandra.service

import com.cloudwick.cassandra.schema.StatusCount
import com.datastax.driver.core.ResultSet

import scala.concurrent.Future

trait StatusCountServiceModule {
  def statusCountService: StatusCountService

  trait StatusCountService {
    def update(statusCount: StatusCount): Future[ResultSet]
    def getCount(id: Int): Future[Option[Long]]
  }
}
