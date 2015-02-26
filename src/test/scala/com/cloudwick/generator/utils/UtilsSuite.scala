package com.cloudwick.generator.utils

import java.text.SimpleDateFormat
import java.util.Date

import org.scalatest.FunSuite

/**
 * Description goes here
 * @author ashrith 
 */
class UtilsSuite extends FunSuite {
  private val utils = new Utils
  private val formatter = new SimpleDateFormat("dd-MMM-yy HH:mm:ss")

  test("Check for pickWeighted()") {
    val map = Map(
      "Men"   -> 50,
      "Women" -> 50
    )
    val totalIterations = 1000
    var menCount = 0
    var womenCount = 0
    (1 to totalIterations).foreach { _ =>
      utils.pickWeightedKey(map) match {
        case "Men" => menCount += 1
        case "Women" => womenCount += 1
      }
    }
    val menPercentage = (menCount * 100) / totalIterations.toDouble
    val womenPercentage = (womenCount * 100) / totalIterations.toDouble
    println("Men Count: " + menCount + " Percentage: " + menPercentage + "%")
    println("Women Count: " + womenCount + " Percentage: " + womenPercentage + "%")
    assert(menPercentage > 45)
    assert(womenPercentage > 45)
  }

  test("Check randInt() is generating integers within range") {
    val rand = utils.randInt(10, 20)
    assert(rand >= 10 && rand <= 20)
  }

  test("Check genDate() is generating proper date with in range") {
    val start = "01-Jan-10 12:00:00"
    val end = "10-Jan-10 12:00:00"
    val startDate = formatter.parse(start)
    val endDate = formatter.parse(end)
    val rand = utils.genDate(start, end)
    val randDate = new Date(rand)
    assert(randDate.after(startDate) && randDate.before(endDate))
  }
}
