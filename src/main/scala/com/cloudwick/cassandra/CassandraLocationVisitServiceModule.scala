package com.cloudwick.cassandra

import com.cloudwick.cassandra.schema.{LocationVisit, LocationVisitRecord}
import com.cloudwick.cassandra.service.LocationVisitServiceModule
import com.cloudwick.logging.LazyLogging
import com.websudos.phantom.Implicits._

import scala.concurrent.Future

/**
 * Description goes here.
 * @author ashrith
 */
trait CassandraLocationVisitServiceModule extends LocationVisitServiceModule with CassandraService {

  object locationVisitService extends LocationVisitService with LazyLogging {

    override def update(locationVisit: LocationVisit) = {
      logger.trace(
        s"Update location visit counter. Country: ${locationVisit.country} " +
        s"City: ${locationVisit.city} " +
        s"Count: ${locationVisit.totalCount}"
      )

      LocationVisitRecord.update
        .where(_.country eqs locationVisit.country)
        .and(_.city eqs locationVisit.city)
        .modify(_.total_count increment locationVisit.totalCount)
        .future()
    }

    def getCount(country: String, city: String): Future[Option[Long]] = {
      LocationVisitRecord
        .select(_.total_count)
        .where(_.country eqs country)
        .and(_.city eqs city)
        .one()(session, cassandraExecutionContext)
    }
  }
}
