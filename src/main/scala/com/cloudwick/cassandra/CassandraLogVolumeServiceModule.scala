package com.cloudwick.cassandra

import com.cloudwick.cassandra.schema.{LogVolume, LogVolumeRecord}
import com.cloudwick.cassandra.service.LogVolumeServiceModule
import com.cloudwick.logging.LazyLogging
import com.websudos.phantom.Implicits._

trait CassandraLogVolumeServiceModule extends LogVolumeServiceModule with CassandraService {

  object logVolumeService extends LogVolumeService with LazyLogging {
    override def update(logVolume: LogVolume) = {
      logger.trace(
        s"Update volume per minute count. Minute: ${logVolume.timeStamp} " +
        s"Count: ${logVolume.totalCount}"
      )

      LogVolumeRecord.update
        .where(_.timeStamp eqs logVolume.timeStamp)
        .modify(_.total_count increment logVolume.totalCount)
        .future()
    }

    override def getCount(id: Long) = {
      LogVolumeRecord
        .select(_.total_count)
        .where(_.timeStamp eqs id)
        .one()(session, cassandraExecutionContext)
    }
  }
}
