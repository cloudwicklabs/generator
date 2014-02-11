package com.cloudwick.generator.osge

import scala.util.Random
import com.cloudwick.generator.utils.Utils

/**
 * Generates users names
 * @author ashrith
 */
class Person {
  private val random = Random
  private val utils = new Utils

  private val GENDERS = Map(
    "male" -> 56,
    "female" -> 44
  )

  private val lastNames = Array(
    "ABEL", "ANDERSON", "ANDREWS", "ANTHONY", "BAKER", "BROWN", "BURROWS", "CLARK", "CLARKE", "CLARKSON", "DAVIDSON",
    "DAVIES", "DAVIS", "DENT", "EDWARDS", "GARCIA", "GRANT", "HALL", "HARRIS", "HARRISON", "JACKSON", "JEFFRIES",
    "JEFFERSON", "JOHNSON", "JONES", "KIRBY", "KIRK", "LAKE", "LEE", "LEWIS", "MARTIN", "MARTINEZ", "MAJOR", "MILLER",
    "MOORE", "OATES", "PETERS", "PETERSON", "ROBERTSON", "ROBINSON", "RODRIGUEZ", "SMITH", "SMYTHE", "STEVENS",
    "TAYLOR", "THATCHER", "THOMAS", "THOMPSON", "WALKER", "WASHINGTON", "WHITE", "WILLIAMS", "WILSON", "YORKE"
  )

  private val maleFirstNames = Array(
    "ADAM", "ANTHONY", "ARTHUR", "BRIAN", "CHARLES", "CHRISTOPHER", "DANIEL", "DAVID", "DONALD", "EDGAR", "EDWARD",
    "EDWIN", "GEORGE", "HAROLD", "HERBERT", "HUGH", "JAMES", "JASON", "JOHN", "JOSEPH", "KENNETH", "KEVIN", "MARCUS",
    "MARK", "MATTHEW", "MICHAEL", "PAUL", "PHILIP", "RICHARD", "ROBERT", "ROGER", "RONALD", "SIMON", "STEVEN", "TERRY",
    "THOMAS", "WILLIAM"
  )

  private val femaleFirstNames = Array(
    "ALISON", "ANN", "ANNA", "ANNE", "BARBARA", "BETTY", "BERYL", "CAROL", "CHARLOTTE", "CHERYL", "DEBORAH", "DIANA",
    "DONNA", "DOROTHY", "ELIZABETH", "EVE", "FELICITY", "FIONA", "HELEN", "HELENA", "JENNIFER", "JESSICA", "JUDITH",
    "KAREN", "KIMBERLY", "LAURA", "LINDA", "LISA", "LUCY", "MARGARET", "MARIA", "MARY", "MICHELLE", "NANCY", "PATRICIA",
    "POLLY", "ROBYN", "RUTH", "SANDRA", "SARAH", "SHARON", "SUSAN", "TABITHA", "URSULA", "VICTORIA", "WENDY"
  )

  // which age group should appear how many times in the data set
  private val AGE_PROBABILITY = Map(
    "18" -> 15,
    "19" -> 12,
    "20" -> 12,
    "21" -> 11,
    "22" -> 11,
    "23" -> 9,
    "24" -> 7,
    "25" -> 6,
    "26" -> 5,
    "27" -> 4,
    "28" -> 3,
    "29" -> 2,
    "30" -> 2
  )

  private val lettersArr = ('A' to 'Z').toList

  val gender = utils.pickWeightedKey(GENDERS)

  val name = gender match {
    case "male" => maleName
    case "female" => femaleName
  }

  val age = utils.pickWeightedKey(AGE_PROBABILITY).toInt

  private def initial = lettersArr(random.nextInt(lettersArr.size))

  private def lastName = lastNames(random.nextInt(lastNames.size))

  private def femaleName = s"${femaleFirstNames(random.nextInt(femaleFirstNames.size))} $initial $lastName"

  private def maleName = s"${maleFirstNames(random.nextInt(maleFirstNames.size))} $initial $lastName"
}