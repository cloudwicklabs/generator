package com.cloudwick.generator.utils

import java.nio.ByteBuffer
import java.util.concurrent.TimeoutException

import com.amazonaws.services.kinesis.model.{PutRecordRequest, PutRecordsRequestEntry}
import io.github.cloudify.scala.aws.kinesis.Client.ImplicitExecution._
import io.github.cloudify.scala.aws.kinesis.Definitions.{PutResult, Stream}
import io.github.cloudify.scala.aws.kinesis.KinesisDsl._
import io.github.cloudify.scala.aws.kinesis.{Client, Requests}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
 * Kinesis Handler
 * @author ashrith
 */
class KinesisHandler(creds: Credentials) extends Handler with LazyLogging {

  private implicit val kinesis = createClient(creds.accessKey, creds.secretKey, creds.endPoint)
  private var stream: Option[Stream] = None

  /**
   * Creates a new kinesis client from provided AWS credentials
   * @param accessKey AWS access Key
   * @param secretKey AWS secret key
   * @param endPoint AWS kinesis end point to connect to
   * @return initialized AmazonKinesisClient
   */
  private[utils] def createClient(accessKey: String,
                                  secretKey: String,
                                  endPoint: String): Client = {
    Client.fromCredentials(accessKey, secretKey, endPoint)
  }

  /**
   * Creates a new kinesis stream if one does not exist
   * @param name name of the stream to create
   * @param size number of shards to support this kinesis stream, in general a single shard can
   *             handle upto 1000 records written per second, up to a maximum total of 1 MB data
   *             written per second
   * @param duration how long to keep checking if the stream became active, in seconds
   * @param retries how many retries have to be made
   * @return Boolean
   */
  def createStream(name: String, size: Int, duration: Int, retries: Int): Boolean = {
    val streamsListF = for {
      s <- Kinesis.streams.list
    } yield s
    val streamList: Iterable[String] = Await.result(streamsListF, Duration(duration, SECONDS))
    streamList.foreach(streamName => {
      if (streamName == name) {
        logger.info("Kinesis stream '{}' already exists.", name)
        logger.trace("Creating stream object")
        val createStreamF = for {
          s <- Kinesis.streams.create(name)
        } yield s
        stream = Some(Await.result(createStreamF, Duration(duration, SECONDS)))
        return true
      }
    })
    logger.info("Attempting to create kinesis stream '{}' with shards '{}'", name, size)
    val createStreamF = for {
      s <- Requests.CreateStream.apply(name, size)
    } yield s

    try {
      stream = Some(Await.result(createStreamF, Duration(duration, SECONDS)))
      logger.debug("Waiting ({} seconds) for kinesis stream '{}' to get created", duration, name)
      Await.result(stream.get.waitActive.retrying(retries), Duration(duration, SECONDS))
    } catch {
      case _: TimeoutException =>
        logger.error("Timed out waiting while create stream '{}'", name)
        false
    }
    logger.info("Successfully created stream '{}'", name)
    true
  }

  override def publishRecord(record: String) = {
    val waitDuration = 5
    try {
      val key = s"pk-${System.currentTimeMillis()}"
      logger.trace(s"Attempting to insert record with partition key: $key")
      val pr = writeRecord(ByteBuffer.wrap(record.getBytes), key, waitDuration)
      logger.trace(s"Inserted record with shardID: ${pr.shardId} and " +
        s"sequenceNumber: ${pr.sequenceNumber}")
    } catch {
      case e: Throwable => logger.error("Error:: {}", e)
    }
  }

  override def publishBuffered(records: ArrayBuffer[String]) = {
    val waitDuration = 10
    try {
      val br = records.map { record =>
        val k = s"pk-${System.currentTimeMillis()}"
        (ByteBuffer.wrap(record.getBytes), k)
      }.toList
      logger.trace("Attempting to insert record(s) with partition key(s): " + br.map(t => t._2))
      val pr = writeBufferedRecords(br, waitDuration)
      logger.trace(s"Inserted record with shardIDs: ${pr.shardIds} and " +
        s"sequenceNumber: ${pr.sequenceNumber}")
    } catch {
      case e: Throwable => logger.error("Error:: {}", e)
    }
  }

  /**
   * Writes a record to a given stream with specified partition key
   * @param record Data blob to store
   * @param partitionKey Partition key for this record. Partition key is used by Amazon Kinesis to
   *                     distribute data across shards. Amazon Kinesis segregates the data records
   *                     that belong to a data stream into multiple shards, using the partition key
   *                     associated with each data record to determine which shard a given data
   *                     record belongs to.
   *                     Partition keys are Unicode strings, with a maximum length limit of 256
   *                     characters for each key.
   * @param duration Time in seconds to wait to put this data
   * @return A PutResult containing the ShardId and SequenceNumber of the record written to.
   */
  private[utils] def writeRecord(record: ByteBuffer,
                                 partitionKey: String,
                                 duration: Int): PutResult = {
    val putData = for {
      p <- stream.get.put(record, partitionKey)
    } yield p
    val putResult = Await.result(putData, Duration(duration, SECONDS))
    putResult
  }

  /**
   * Writes a list of records to a given stream
   * @param records List of records to put in Tuple(data, partitionKey) format
   * @param duration Time in seconds to wait to put this data
   * @return A PutResult containing the ShardId and SequenceNumber of the record written to.
   */
  private[utils] def writeBufferedRecords(records: List[(ByteBuffer, String)], duration: Int) = {
    val putData = for {
      p <- stream.get.multiPut(records)
    } yield p
    val putResult = Await.result(putData, Duration(duration, SECONDS))
    putResult
  }
}
