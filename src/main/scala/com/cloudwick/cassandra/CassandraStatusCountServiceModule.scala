package com.cloudwick.cassandra

import com.cloudwick.cassandra.schema.{StatusCount, StatusCountRecord}
import com.cloudwick.cassandra.service.StatusCountServiceModule
import com.cloudwick.logging.Logging
import com.websudos.phantom.Implicits._

import scala.concurrent.Future

trait CassandraStatusCountServiceModule extends StatusCountServiceModule with CassandraService {

  object statusCountService extends StatusCountService with Logging {

    override def update(statusCount: StatusCount) = {
      logger.trace(
        s"Update status count counter. StatusCode: ${statusCount.statusCode} " +
        s"Count: ${statusCount.totalCount}"
      )

      StatusCountRecord.update
        .where(_.statusCode eqs statusCount.statusCode)
        .modify(_.total_count increment statusCount.totalCount)
        .future()
    }

    override def getCount(id: Int) = {
      StatusCountRecord
        .select(_.total_count)
        .where(_.statusCode eqs id)
        .one()(session, cassandraExecutionContext)
    }
  }

}
