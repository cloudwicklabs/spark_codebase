package com.cloudwick.cassandra.schema

import com.datastax.driver.core.Row
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.Implicits._

/**
 * Counts the number of visits by country and city
 * @param country    iso code for the country
 * @param city       city name
 * @param totalCount number of times a unique visit has came from country, city
 */
case class LocationVisit(country: String, city: String, totalCount: Long)

sealed class LocationVisitRecord extends CassandraTable[LocationVisitRecord, LocationVisit] {

  object country      extends StringColumn(this)  with PartitionKey[String]
  object city         extends StringColumn(this)  with PrimaryKey[String]
  object total_count  extends CounterColumn(this)

  override def fromRow(row: Row): LocationVisit = {
    LocationVisit(country(row), city(row), total_count(row))
  }
}

object LocationVisitRecord extends LocationVisitRecord {
  override val tableName = "location_visits_counts"
}