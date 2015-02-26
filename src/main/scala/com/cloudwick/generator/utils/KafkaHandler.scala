package com.cloudwick.generator.utils

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * Kafka handler to write data to handler
 * @author ashrith 
 */
class KafkaHandler(val brokerList: String, val topicName: String) {
  lazy val logger = LoggerFactory.getLogger(getClass)
  private val props = new Properties()
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("metadata.broker.list", brokerList)
  private val config = new ProducerConfig(props)
  private var producer: Producer[String, String] = null

  try {
    if (producer == null) {
      logger.debug("Attempting to make connection with kafka")
      producer = new Producer[String, String](config)
    } else {
      logger.debug("Reusing the kafka connection")
    }
  } catch { case _: Throwable => () }

  def close() = {
    try {
      logger.debug("Attempting to close the producer stream to kafka")
      producer.close()
    } catch { case _: Throwable => () }
  }

  def send(keys: KeyedMessage[String, String]) = {
    try {
      // logger.debug("Attempting to send the key to kafka broker")
      producer.send(keys)
    }
  }

  def publish(record: String) = {
    try {
      send(KeyedMessage[String, String](topicName, java.util.UUID.randomUUID().toString, record))
    } catch {
      case e: Throwable => logger.error("Error:: {}", e)
    }
  }

  def publishBuffered(records: ArrayBuffer[String]) = {
    try {
      records.foreach { record =>
        send(KeyedMessage[String, String](topicName, java.util.UUID.randomUUID().toString, record))
      }
    } catch {
      case e: Throwable => logger.error("Error:: {}", e)
    }
  }
}
