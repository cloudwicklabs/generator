package com.cloudwick.generator.utils

import java.io.File

import org.apache.avro.{file, Schema}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.DatumWriter

import scala.collection.mutable.ArrayBuffer

/**
 * Utilities for handling files
 * @author ashrith 
 */
class FileUtils {
  /**
   * Writes to a file using java print writer
   * @param f java file object
   * @param op java print writer object
   * @return
     *
   * Usage:
   * {{{
   * import java.io._
   * val data = Array("Five","strings","in","a","file!")
   * writeToFile(new File("example.txt"))(p => {
   *  data.foreach(p.println)
   * })
   * }}}
   */
  def writeToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  /**
   * Writes to a file using java file writer
   * @param f java file object
   * @param op java buffered writer object
   * @return
     *
   * Usage:
   * {{{
   * import java.io._
   * Usage: writeBufferedToFile(new FileWriter(new File("example.txt")))(bw => {
   *   data.foreach(bw.println)
   * })
   * }}}
   */
  def writeBufferedToFile(f: java.io.File)(op: java.io.BufferedWriter => Unit) {
    val bw = new java.io.BufferedWriter(new java.io.FileWriter(f))
    try { op(bw) } finally { bw.close() }
  }

  /**
   * Writes list of records to a specified file object with specified buffer size
   * @param file object
   * @param records list of records to write
   * @param bufferSize to use for the buffered writer
   */
  def writeBuffered(file: java.io.File, records: ArrayBuffer[String], bufferSize: Int) {
    val fw = new java.io.FileWriter(file)
    val bw = new java.io.BufferedWriter(fw, bufferSize)
    try {
      records.foreach { record =>
        bw.write(record)
      }
      bw.flush()
    } finally {
      bw.close()
      fw.close()
    }
  }

  /**
   * Initializes avro file with specified avro schema
   * @param dest file object
   * @param schemaDesc avro schema
   * @return DataFileWriter
   */
  def initializeAvroFile(dest: File, schemaDesc: String): DataFileWriter[GenericRecord] = {
    val schema = new Schema.Parser().parse(schemaDesc)
    val writer:DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter:DataFileWriter[GenericRecord] = new file.DataFileWriter[GenericRecord](writer)
    dataFileWriter.create(schema, dest)
  }

  /**
   * Closes avro file descriptor
   * @param dataFileWriter object
   */
  def closeAvroFile(dataFileWriter: DataFileWriter[GenericRecord]) {
    if (dataFileWriter != null) {
      println("Closing avro file descriptor")
      dataFileWriter.close()
    }
  }
}