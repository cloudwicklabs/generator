package com.cloudwick.generator.utils

import scala.collection.mutable.ArrayBuffer

/**
 * Handler for all external write systems
 * @author ashrith
 */
trait Handler {
  /**
   * Publish/persist a single record to the specified external system
   * @param record A record to persist
   */
  def publishRecord(record: String)

  /**
   * Publish/persist a multiple record to the specified external system
   * @param records Collection for records
   */
  def publishBuffered(records: ArrayBuffer[String])
}
