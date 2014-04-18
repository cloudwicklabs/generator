package com.cloudwick.generator.utils

import org.slf4j.LoggerFactory
import java.util.Properties
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import org.apache.avro.generic.GenericRecord
import kafka.message.Message
import java.io.ByteArrayOutputStream
import org.apache.avro.io.{EncoderFactory, BinaryEncoder}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.commons.io.IOUtils
import scala.collection.mutable.ArrayBuffer
import org.apache.avro.Schema
import org.apache.avro.io.Encoder
import org.apache.avro.io.DatumWriter
import java.nio.ByteBuffer

//import com.cloudwick.generator.model.LogEvent


/**
 * Kafka handler to write data to handler
 * @author ashrith 
 */
class KafkaAvroHandler(val brokerList: String, val schemaDesc: String, val topicName: String) {
  lazy val logger = LoggerFactory.getLogger(getClass)
  private val props = new Properties()
  props.put("serializer.class", "kafka.serializer.DefaultEncoder")
  props.put("metadata.broker.list", brokerList)
  private val config = new ProducerConfig(props)
  private var producer: Producer[String, Message] = null

  private val bout: ByteArrayOutputStream = new ByteArrayOutputStream()
  private val avroEncoder: Encoder = EncoderFactory.get().binaryEncoder(bout, null)
  private val schema = new Schema.Parser().parse(schemaDesc)
  private val writer: DatumWriter[GenericRecord] = new SpecificDatumWriter[GenericRecord](schema)
  // private static final SpecificDatumWriter<Event> avroEventWriter = new SpecificDatumWriter<Event>(Event.SCHEMA$);
  // private val avroEventWriter = new SpecificDatumWriter[GenericRecord](schema)

  try {
    if (producer == null) {
      logger.debug("Attempting to make connection with kafka")
      producer = new Producer[String, Message](config)
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

  // Key and Value to send
  def send(keys: KeyedMessage[String, Message]) = {
    try {
      // logger.debug("Attempting to send the key to kafka broker")
      producer.send(keys)
    }
  }

  def publish(event: GenericRecord) = {
    try {
      writer.write(event, avroEncoder)
      avroEncoder.flush()
      send(KeyedMessage[String, Message](topicName, java.util.UUID.randomUUID().toString, new Message(bout.toByteArray)))
    } catch {
      case e: Throwable => logger.error("Error:: {}", e)
    }
  }

  def publishBuffered(events: ArrayBuffer[GenericRecord]) = {
    try {
      events.foreach { event =>
        logger.info("event: {}", event)
        writer.write(event, avroEncoder)
        avroEncoder.flush()
        logger.info("bevent: {}", bout.toByteArray)
        //send(KeyedMessage[String, Message](topicName, java.util.UUID.randomUUID().toString, new Message(bout.toByteArray)))
        producer.send(new KeyedMessage[String, Message](topicName, new Message(ByteBuffer.wrap(bout.toByteArray))))
        // producer.send(new KeyedMessage[String, Message](topicName, bout.toString))
      }
    } catch {
      case e: Throwable => logger.error("Error:: {}", e)
    }
  }
}
