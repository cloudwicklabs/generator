package com.cloudwick.generator.utils

import java.io.{BufferedInputStream, File, FileInputStream}

import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericDatumReader

/**
 * Convenience Avro utilities
 * @author ashrith
 */
object AvroUtils extends LazyLogging {

  /**
   * Reads a avro file and prints the output to console
   * @param input avro file to read from
   */
  def readAvroFile(input: File) = {
    val inputStream: BufferedInputStream = new BufferedInputStream(new FileInputStream(input))
    val reader: GenericDatumReader[Object] = new GenericDatumReader[Object]()
    val streamReader: DataFileStream[Object] = new DataFileStream[Object](inputStream, reader)
    var breakCount = 0

    try {
      val schema: Schema = streamReader.getSchema
      println("Here are the records limit (100)")
      println("Schema: " + schema)

      while (streamReader.hasNext) {
        breakCount += 1
        println(streamReader.next())
        if (breakCount == 100) {
          System.exit(1)
        }
      }
    }
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      logger.error("Usage: AvroUtils [command] [argument]")
      System.exit(1)
    }

    args(0) match {
      case "read" | "READ" =>
        readAvroFile(new File(args(1)))
    }
  }

}
