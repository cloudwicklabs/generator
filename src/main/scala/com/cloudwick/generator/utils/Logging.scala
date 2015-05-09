package com.cloudwick.generator.utils

import org.slf4j.{Logger, LoggerFactory}

/**
 * Base Logging trait
 */
trait Logging {
  protected def logger: Logger
}

/**
 * Adds the lazy val `logger` of to the class into which this trait is mixed.
 */
trait LazyLogging extends Logging {
  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
}

/**
 * Adds the non-lazy val `logger` to the class into which this trait is mixed.
 */
trait StrictLogging extends Logging {
  protected val logger: Logger = LoggerFactory.getLogger(getClass.getName)
}
