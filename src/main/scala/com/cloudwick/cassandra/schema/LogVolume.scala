package com.cloudwick.cassandra.schema

import com.cloudwick.cassandra.CassandraService
import com.datastax.driver.core.Row
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.Implicits._
import com.websudos.phantom.keys.PartitionKey
import org.joda.time.DateTime

/**
 * Count log events by minute
 * @param timeStamp   time stamp rounded off to minute in epoch format
 * @param totalCount  number of times events have appeared in that minute window
 */
case class LogVolume(timeStamp: Long, totalCount: Long)

/**
 * Seal the class and only allow importing of the companion object
 */
sealed class LogVolumeRecord extends CassandraTable[LogVolumeRecord, LogVolume] with Serializable {
  override val tableName: String = "log_volume"

  object timeStamp   extends LongColumn(this) with PartitionKey[Long] {
    override lazy val name = "time_stamp"
  }
  object total_count extends CounterColumn(this)

  // Mapping function, transforming a row into custom type
  override def fromRow(row: Row): LogVolume = {
    LogVolume(timeStamp(row), total_count(row))
  }
}

object LogVolumeRecord extends LogVolumeRecord {
  // rename the table name in the schema
  override val tableName = "log_volume_minute"
}