package com.cloudwick.generator.utils

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.Properties

import kafka.message.Message
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.avro.io.{DatumWriter, Encoder, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumWriter, SpecificRecord}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


/**
 * Kafka handler to write data to handler
 * @author ashrith 
 */
class KafkaAvroHandler[T <: SpecificRecord](brokerList: String,
                                            topicName: String)(implicit t: ClassTag[T])
  extends AvroHandler[T] with LazyLogging {

  private val props = new Properties()
  props.put("serializer.class", "kafka.serializer.DefaultEncoder")
  props.put("metadata.broker.list", brokerList)
  private val config = new ProducerConfig(props)
  private var producer: Producer[String, Message] = null

  private val bout: ByteArrayOutputStream = new ByteArrayOutputStream()
  private val avroEncoder: Encoder = EncoderFactory.get().binaryEncoder(bout, null)
  private val writer: DatumWriter[T] = new SpecificDatumWriter[T](
    t.runtimeClass.asInstanceOf[Class[T]]
  )

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
      producer.send(keys)
    }
  }

  def publish(datum: T) = {
    try {
      writer.write(datum, avroEncoder)
      avroEncoder.flush()
      send(KeyedMessage[String, Message](topicName, java.util.UUID.randomUUID().toString, new Message(bout.toByteArray)))
    } catch {
      case e: Throwable => logger.error("Error:: {}", e)
    }
  }

  def publishBuffered(datums: ArrayBuffer[T]) = {
    try {
      datums.foreach { datum =>
        writer.write(datum, avroEncoder)
        avroEncoder.flush()
        producer.send(new KeyedMessage[String, Message](topicName, new Message(ByteBuffer.wrap(bout.toByteArray))))
      }
    } catch {
      case e: Throwable => logger.error("Error:: {}", e)
    }
  }
}
