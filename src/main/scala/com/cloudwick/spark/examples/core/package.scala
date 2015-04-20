package com.cloudwick.spark.examples

/**
 * Package object
 */
package object core {
  implicit class StringUtils(val value: String) {
    def strip(stripChars: String): String = value.stripPrefix(stripChars).stripSuffix(stripChars)
  }
}