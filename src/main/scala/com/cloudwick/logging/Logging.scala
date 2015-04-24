package com.cloudwick.logging

import org.slf4j.{LoggerFactory, Logger}

trait LazyLogging {
  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
}
