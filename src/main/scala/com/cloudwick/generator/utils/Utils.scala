package com.cloudwick.generator.utils

import scala.util.Random
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer
import java.io.File
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.{file, Schema}
import org.apache.avro.io.DatumWriter
import java.util.Calendar
import java.text.SimpleDateFormat

/**
 * Set of utilities used in generating data
 * @author ashrith
 */
class Utils {
  private val logger = LoggerFactory.getLogger(getClass)
  private val formatter = new SimpleDateFormat("dd-MMM-yy HH:mm:ss")

  /**
   * Picks an element out of map, following the weight as the probability
   * @param map map of key value pairs having key to picks and its value as the probability
   *            of picking that key
   * @return randomly picked key selected from map
   */
  def pickWeightedKey(map: Map[String, Int]): String = {
    var total = 0
    map.values.foreach { weight => total += weight }
    val rand = Random.nextInt(total)
    var running = 0
    for((key, weight) <- map) {
      if(rand >= running && rand < (running + weight)) {
        return key
      }
      running += weight
    }
    map.keys.head
  }

  /**
   * Returns a random number between min and max, inclusive.
   * The difference between min and max can be at most `Integer.MAX_VALUE - 1`
   * @param min Minimum value
   * @param max Maximum value
   * @return Integer between min and max, inclusive
   */
  def randInt(min: Int, max: Int) = {
    Random.nextInt((max - min) + 1) + min
  }

  /**
   * Generates random date between a given range of dates
   * @param from date range from which to generate the date from, should be of the format
   *             'dd-MMM-yy HH:mm:ss'
   * @param to date range end, should be of the format 'dd-MMM-yy HH:mm:ss'
   * @return a random date in milli seconds
   */
  def genDate(from: String, to: String) = {
    val cal = Calendar.getInstance()
    cal.setTime(formatter.parse(from))
    val v1 = cal.getTimeInMillis
    cal.setTime(formatter.parse(to))
    val v2 = cal.getTimeInMillis

    val diff: Long = (v1 + Math.random() * (v2 - v1)).toLong
    cal.setTimeInMillis(diff)
    cal.getTimeInMillis
  }

  /**
   * Measures time took to run a block
   * @param block code block to run
   * @param message additional message to print
   * @tparam R type
   * @return returns block output
   */
  def time[R](message: String = "code block")(block: => R): R = {
    val s = System.nanoTime
    // block: => R , implies call by name i.e, the execution of block is delayed until its called by name
    val ret = block
    logger.info("Time elapsed in " + message + " : " +(System.nanoTime - s)/1e6+"ms")
    ret
  }

  /**
   * Utilities for handling files
   */
  class FileUtils {
    /**
     * Writes to a file using java print writer
     * @param f java file object
     * @param op java print writer object
     * @return
     *
     * Usage:
     * import java.io._
     * val data = Array("Five","strings","in","a","file!")
     * writeToFile(new File("example.txt"))(p => {
     *  data.foreach(p.println)
     * })
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
     * import java.io._
     * Usage: writeBufferedToFile(new FileWriter(new File("example.txt")))(bw => {
     *   data.foreach(bw.println)
     * })
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
}