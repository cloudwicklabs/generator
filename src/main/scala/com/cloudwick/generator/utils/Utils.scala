package com.cloudwick.generator.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.slf4j.LoggerFactory

import scala.util.Random

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
    logger.info("Time elapsed in " + message + " : " + (System.nanoTime - s) / 1e6 + "ms")
    ret
  }
}