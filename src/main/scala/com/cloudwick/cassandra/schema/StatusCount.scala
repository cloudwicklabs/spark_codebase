package com.cloudwick.cassandra.schema

import com.datastax.driver.core.Row
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.Implicits._

/**
 * Counts http status code's returned by the web server
 * @param statusCode  type of the status code 200, 404, 503, ...
 * @param totalCount  number of times a status code has appeared in its entirety
 */
case class StatusCount(statusCode: Int, totalCount: Long)

sealed class StatusCountRecord extends CassandraTable[StatusCountRecord, StatusCount] {
  object statusCode extends IntColumn(this)     with PartitionKey[Int]
  object total_count    extends CounterColumn(this)

  override def fromRow(row: Row): StatusCount = {
    StatusCount(statusCode(row), total_count(row))
  }
}

object StatusCountRecord extends StatusCountRecord {
  override val tableName = "status_count"
}