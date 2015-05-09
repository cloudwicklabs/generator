package com.cloudwick.generator.utils

import scala.collection.mutable.ArrayBuffer

/**
 * Handler for all external write systems
 * @author ashrith
 */
trait Handler {
  /**
   * Closes the stream
   */
  def close()
}
