package com.cloudwick.generator.utils

import scala.collection.mutable.ArrayBuffer

/**
 * Base trait for all text based writers
 * @author ashrith
 */
trait TextHandler extends Handler {
  /**
   * Publish/persist a single record to the specified external system
   * @param record A record to persist
   */
  def publish(record: String)

  /**
   * Publish/persist a multiple record to the specified external system
   * @param records Collection for records
   */
  def publishBuffered(records: ArrayBuffer[String])
}
