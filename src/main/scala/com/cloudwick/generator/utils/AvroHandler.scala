package com.cloudwick.generator.utils

import org.apache.avro.generic.GenericRecord

import scala.collection.mutable.ArrayBuffer

/**
 * Description goes here.
 * @author ashrith
 */
trait AvroHandler[T] extends Handler {
  /**
   * Publish/persist a single record to the specified external system
   * @param datum Avro's GenericRecord to write
   */
  def publish(datum: T)

  /**
   * Publish/persist a multiple record to the specified external system
   * @param datums Collection for GenericRecord's
   */
  def publishBuffered(datums: ArrayBuffer[T])
}
