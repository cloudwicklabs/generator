package com.cloudwick.generator.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTimeConstants, Period, Days, LocalDate}

class DateUtils extends LazyLogging {
  private lazy val formatter = new SimpleDateFormat("dd-MMM-yy HH:mm:ss")
  private lazy val jodaFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

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
   * Get number of days between the given range of dates
   * @param from start date
   * @param until end date
   * @return Int
   *
   * Usage:
   *    println(getDates(df.parseLocalDate("2015-07-01"), df.parseLocalDate("2015-07-08")))
   */
  def getDates(from: LocalDate, until: LocalDate) = {
    Days.daysBetween(from, until).getDays
  }

  /**
   * Generates a range of dates with in the provided start & end dates with step period
   * @param from start date
   * @param to end date
   * @param step step period could be days, weeks, months based on Period()
   * @return Iterator of dates
   *
   * Usage:
   *    dateRange(df.parseLocalDate("2015-07-01"), df.parseLocalDate("2015-07-08"), new Period().withDays(1))
   */
  def dateRange(from: LocalDate, to: LocalDate, step: Period) = {
    Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))
  }

  /**
   * Takes in date string and returns the sunday in that week
   * @param date any date of the week from which you want to fetch the sunday
   */
  def getSundayInWeek(date: String) = {
    jodaFormatter.parseLocalDate(date).withDayOfWeek(DateTimeConstants.SUNDAY)
  }

  /**
   * Fetch the current date in the LocalDate format
   * @return
   */
  def getCurrentDate = {
    new LocalDate().toString(jodaFormatter)
  }

  /**
   * Adds specified number of days to the given date
   * @param date
   * @param daysToAdd
   * @return
   */
  def addDays(date: String, daysToAdd: Int) = {
    jodaFormatter.parseLocalDate(date).plusDays(daysToAdd).toString(jodaFormatter)
  }

  /**
   * Converts date string to Joda LocalDate
   * @param date
   */
  def parseLocalDate(date: String) = {
    jodaFormatter.parseLocalDate(date)
  }

}
