package com.cloudwick.cassandra.service

import com.cloudwick.cassandra.schema.LocationVisit
import com.datastax.driver.core.ResultSet

import scala.concurrent.Future

trait LocationVisitServiceModule {
  def locationVisitService: LocationVisitService

  trait LocationVisitService {
    def update(locationVisit: LocationVisit): Future[ResultSet]
    def getCount(country: String, city: String): Future[Option[Long]]
  }
}
