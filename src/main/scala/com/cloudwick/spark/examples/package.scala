package com.cloudwick.spark

/**
 * Package object
 * @author ashrith
 */
package object examples {
  implicit class StringUtils(val value: String) {
    def strip(stripChars: String): String = value.stripPrefix(stripChars).stripSuffix(stripChars)
  }
}