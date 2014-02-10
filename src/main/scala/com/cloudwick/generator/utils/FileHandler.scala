package com.cloudwick.generator.utils

import java.io.{FileOutputStream, OutputStreamWriter, Writer, File}
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory

/**
 * File handler with inbuilt capability to roll file's and is thread safe
 * @author ashrith 
 */
class FileHandler(val fileName: String, val maxFileSizeBytes: Int, val append: Boolean = false) {
  lazy val logger = LoggerFactory.getLogger(getClass)
  private var stream: Writer = null
  private var openTime: Long = 0
  private var bytesWrittenToFile: Long = 0

  if (new File(fileName).exists()) {
    roll()
  } else {
    openFile()
  }

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
    stream = new OutputStreamWriter(new FileOutputStream(fileName, append), "UTF-8")
    openTime = System.currentTimeMillis()
    bytesWrittenToFile = 0
  }

  def roll() = synchronized {
    logger.debug("Attempting to roll file")
    if (stream ne null) stream.close()
    val n = fileName.lastIndexOf('.')
    val newFileName = if (n > 0) {
      fileName.substring(0, n) + "-" + timeSuffix + fileName.substring(n)
    } else {
      fileName + "-" + timeSuffix
    }
    new File(fileName).renameTo(new File(newFileName))
    openFile()
  }

  def publish(record: String) = {
    try {
      val lineSizeBytes = record.getBytes("UTF-8").length
      synchronized {
        if (bytesWrittenToFile + lineSizeBytes > maxFileSizeBytes) {
          roll()
        }
        stream.write(record)
        stream.flush()
        bytesWrittenToFile += lineSizeBytes
      }
    } catch {
      case e: Throwable => logger.error("Error:: {}", e)
    }
  }

  def publishBuffered(records: ArrayBuffer[String]) = {
    var lineSizeBytes: Int = 0
    try {
      synchronized {
        records.foreach { record =>
          lineSizeBytes = record.getBytes("UTF-8").length
          if (bytesWrittenToFile + lineSizeBytes > maxFileSizeBytes) {
            roll()
          }
          stream.write(record)
          bytesWrittenToFile += lineSizeBytes
        }
        stream.flush()
      }
    } catch {
      case e: Throwable => logger.error("Error:: {}", e)
    }
  }
}
