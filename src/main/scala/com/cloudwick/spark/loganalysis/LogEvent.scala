package com.cloudwick.spark.loganalysis

import org.joda.time.DateTime

/**
 * Wrapper for representing log event
 *
 * @param ip ip address of the request originator
 * @param clientIdentity RFC 1413 identity of the client determined by `identd` on the client
 *                       machine
 * @param userId UsedId of the person requesting the document as determined by the http
 *               authentication
 * @param timeStamp time at which the request was received
 * @param requestType indicates info about http method used by the client is GET
 * @param requestPage client requested resource /test.php
 * @param responseCode status code that the server sends back to the client
 * @param responseSize size of the object returned to the client, not including the response headers
 * @param referrer identifies the site that the client reports having been referred from
 * @param userAgent user-agent HTTP request header, identifies information that the client browser
 *                  reports about itself
 */
case class LogEvent(ip: String,
                    clientIdentity: String,
                    userId: String,
                    timeStamp: DateTime,
                    requestType: String,
                    requestPage: String,
                    responseCode: Int,
                    responseSize: Int,
                    referrer: String,
                    userAgent: String)

/**
 * Wrapper for representing status code counter
 * @param status http web server status code for a request (200, 404, 503, ...)
 * @param count number of events
 */
case class StatusCount(status: Int, count: Long)

/**
 * Wrapper for representing volume counts in minute
 * @param timeStamp epoch minute format
 * @param count number of hits
 */
case class VolumeCount(timeStamp: Long, count: Long)

/**
 * Wrapper for representing location from where the web request originated from
 * @param ip ip address of the lookup
 * @param country originating country
 * @param city originating city
 * @param lat latitude
 * @param lon longitude
 */
case class Location(ip: String, country: String, city: String, lat: Double, lon: Double)

/**
 * Wrapper for representing country counts
 * @param country countries iso code
 * @param count number of times a country has appeared in batch
 */
case class CountryCount(country: String, count: Long)
