package com.cloudwick.generator.utils

import java.io.File
import java.text.SimpleDateFormat

import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.{SpecificDatumWriter, SpecificRecord}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * File handler with inbuilt capability to roll file's and is thread safe
 * @author ashrith 
 */
class AvroFileHandler[T <: SpecificRecord](
    fileName: String,
    maxFileSizeBytes: Int,
    append: Boolean = false)
    (implicit t: ClassTag[T])
  extends AvroHandler[T]
  with LazyLogging {

  private var stream: DataFileWriter[T] = null
  //private val schema = new Schema.Parser().parse(schemaDesc)
  private val schema = t.runtimeClass.newInstance.asInstanceOf[T].getSchema
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
    stream = new org.apache.avro.file.DataFileWriter[T](
      new SpecificDatumWriter[T](t.runtimeClass.asInstanceOf[Class[T]])
    )
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

  def publish(datum: T) = {
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

  def publishBuffered(datums: ArrayBuffer[T]) = {
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
