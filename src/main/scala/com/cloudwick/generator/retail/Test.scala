package com.cloudwick.generator.retail

import java.util.Locale

import com.cloudwick.generator.utils.Utils
import org.joda.time.format.DateTimeFormat
import org.joda.time._

import scala.util.Random

object Test {

  lazy val df = DateTimeFormat.forPattern("yyyy-MM-dd")
  lazy val utils = new Utils

  /**
   * Get number of days between the given range of dates
   * @param from
   * @param until
   * @return
   *  Usage:
   *    val startDate = "2015-07-01"
   *    val endDate = "2015-07-08"
   *    println(getDates(df.parseLocalDate(startDate), df.parseLocalDate(endDate)))
   */
  def getDates(from: LocalDate, until: LocalDate) = {
    Days.daysBetween(from, until).getDays
  }

  /**
   * Generates a range of dates with in the provided start, end and step period
   * @param from
   * @param to
   * @param step
   * @return
   *
   * Usage:
   *    val range = dateRange(df.parseLocalDate(startDate), df.parseLocalDate(endDate), new Period().withDays(1))
   *    range.toList
   */
  def dateRange(from: LocalDate, to: LocalDate, step: Period) = {
    Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))
  }

  def formatString(sId: Int, pId: Int, date: LocalDate, dayOfWeek: String, inventory: Int, unitSale: Int): Unit = {
    println(s"SID: $sId, PID: $pId, DATE: $date($dayOfWeek), INVENTORY: $inventory, UNITSALE: $unitSale")
  }

  def main(args: Array[String]) {
    val storeIds = 2
    val productIdsPerStore = 5
    val startDate = "2015-07-01"
    val startDateSunday = df.parseLocalDate(startDate).withDayOfWeek(DateTimeConstants.SUNDAY)
    val endDate = "2015-07-15"
    println( s"Total days: ${getDates(startDateSunday, df.parseLocalDate(endDate))}" )

    val range = dateRange(startDateSunday, df.parseLocalDate(endDate), new Period().withDays(1))

    val rangeList = range.toList

    println(rangeList)

    println(new LocalDate().toString(df))

    var inventory = 0 // this will be reset based on day of the week
    var unitSale = 0

    var counter = 0

    (1 to storeIds).foreach { sId =>
      (1 to productIdsPerStore).foreach { pId =>
        rangeList.foreach { d =>
          val dayOfWeek = d.dayOfWeek().getAsText(Locale.ENGLISH)
          // println(s"Its: $dayOfWeek")
          dayOfWeek match {
            case "Sunday" =>
              // On Sunday inventory gets replenished
              inventory = 100
              unitSale = utils.randInt(0, inventory)
            case _ =>
              if (inventory > 0) {
                inventory = inventory - unitSale
                unitSale = utils.randInt(0, inventory)
              } else {
                inventory = 0
                unitSale = 0
              }
          }
          // println( s"date: $d, inventory: $inventory, unitsale: $unitSale" )
          // formatString(sId, pId, d, dayOfWeek, inventory, unitSale)
          counter += 1
        }
      }
    }
    println(s"counter: $counter")
    println(s"cal: ${storeIds*productIdsPerStore*rangeList.size}")
  }
}
