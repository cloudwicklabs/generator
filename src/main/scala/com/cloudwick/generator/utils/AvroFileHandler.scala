package com.cloudwick.generator.utils

import java.io.File
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.Schema
import org.slf4j.LoggerFactory

/**
 * File handler with inbuilt capability to roll file's and is thread safe
 * @author ashrith 
 */
class AvroFileHandler(val fileName: String, val schemaDesc: String, val maxFileSizeBytes: Int, val append: Boolean = false) {
  lazy val logger = LoggerFactory.getLogger(getClass)
  private var stream: DataFileWriter[GenericRecord] = null
  private val schema = new Schema.Parser().parse(schemaDesc)
  private var openTime: Long = 0
  private var bytesWrittenToFile: Long = 0

  openFile()

  def flush() = {
    stream.flush()
  }

  def close() = {
    flush()
    try {
      logger.debug("Attempting to close the file {}", fileName)
      stream.close()
    } catch { case _: Throwable => () }
  }

  def timeSuffix = {
    val dateFormat =  new SimpleDateFormat("yyyyMMdd_HHmmss")
    dateFormat.format(new java.util.Date)
  }

  def openFile() = {
    logger.debug("Attempting to open the file {}", fileName)
    val dir = new File(fileName).getParentFile
    if ((dir ne null) && !dir.exists) dir.mkdirs
    stream = new org.apache.avro.file.DataFileWriter[GenericRecord](new GenericDatumWriter[GenericRecord](schema))
    if (append) {
      stream.appendTo(new File(fileName))
    } else {
      stream.create(schema, new File(fileName))
    }
    openTime = System.currentTimeMillis()
    bytesWrittenToFile = 0
  }

  def roll() = synchronized {
    logger.debug("Attempting to roll file")
    stream.close()
    val n = fileName.lastIndexOf('.')
    val newFileName = if (n > 0) {
      fileName.substring(0, n) + "-" + timeSuffix + fileName.substring(n)
    } else {
      fileName + "-" + timeSuffix
    }
    new File(fileName).renameTo(new File(newFileName))
    openFile()
  }

  def publish(datum: GenericRecord) = {
    try {
      val lineSizeBytes = datum.toString.getBytes("UTF-8").length // this is not dependable
      synchronized {
        if (bytesWrittenToFile + lineSizeBytes > maxFileSizeBytes) {
          roll()
        }
        stream.append(datum)
        stream.flush()
        bytesWrittenToFile += lineSizeBytes
      }
    } catch {
      case e: Throwable => logger.error("Error:: {}", e)
    }
  }

  def publishBuffered(datums: ArrayBuffer[GenericRecord]) = {
    var lineSizeBytes: Int = 0
    try {
      synchronized {
        datums.foreach { datum =>
          lineSizeBytes = datum.toString.getBytes("UTF-8").length
          if (bytesWrittenToFile + lineSizeBytes > maxFileSizeBytes) {
            roll()
          }
          stream.append(datum)
          bytesWrittenToFile += lineSizeBytes
        }
        stream.flush()
      }
    } catch {
      case e: Throwable => logger.error("Error:: {}", e)
    }
  }
}
