package com.cloudwick.generator.osge

import com.cloudwick.generator.utils.{DateUtils, Utils}
import scala.util.Random
import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Generates customer records with companion object
 * @author ashrith
 */

object Customers {
  // which country should occur at what probability
  private val COUNTRY_PROBABILITY = Map(
    "USA"      -> 60,
    "UK"       -> 25,
    "CANADA"   -> 5,
    "MEXICO"   -> 5,
    "GERMANY"  -> 10,
    "FRANCE"   -> 10,
    "EGYPT"    -> 5
  )

  // game most played by females
  val GAMES_FEMALE_PROBABILITY = Map(
    "city"       -> 50,
    "pictionary" -> 30,
    "scramble"   -> 15,
    "sniper"     -> 5
  )

  // game most played by males
  val GAMES_MALE_PROBABILITY = Map(
    "sniper"     -> 70,
    "scramble"   -> 20,
    "pictionary" -> 10,
    "city"       -> 10
  )
}

class Customers(cId: String, cName: String, cGender: String) {
  private val utils = new Utils
  private val dateUtils = new DateUtils
  private val random = Random
  private val formatter = new SimpleDateFormat("dd-MMM-yy HH:mm:ss")

  /*
   * Accessor(s)
   */
  val custId = cId
  val custName = cName
  val custEmail = genEmail
  val registerDate = dateUtils.genDate("01-Jan-10 12:10:00", formatter.format(Calendar.getInstance().getTimeInMillis))
  val custCountry = utils.pickWeightedKey(Customers.COUNTRY_PROBABILITY)
  val custAddress = custCountry match {
    case "USA" => new Address().toString
    case _ => "N/A"
  }
  // users who pay will have a average friends count of > 10
  private val paidCustomerFriendCount = 10
  // maximum friends each user can have
  private val maxFriendCountRange = 500
  // 40 % of customers are paid
  private val paidCustomerPercent = 0.4
  // set 60% of the customers life time to < 10 days and others to 10-100 days
  val custLifeTime = if (random.nextFloat() <  0.6) {
                          random.nextInt(10)
                         } else {
                          utils.randInt(10, 100)
                         }
  // 30% of the users don't have any friends at all
  val custFriendCount = if (random.nextFloat() < 0.3) {
                          0
                        } else {
                          // 40% of users will have fried count > 5 and other will be friend < 5
                          if (random.nextFloat() < 0.4) {
                            utils.randInt(paidCustomerFriendCount, maxFriendCountRange)
                          } else {
                            random.nextInt(paidCustomerFriendCount)
                          }
                        }
  // users who have friend count > paidCustomerFriendCount and total life time in the site > 20 are paid subcribers
  val paidSubscriber =  if (custFriendCount > paidCustomerFriendCount && custLifeTime > 20) {
                          if (random.nextFloat() < paidCustomerPercent) {
                            "yes"
                          } else {
                            "no"
                          }
                        } else {
                          "no"
                        }

  val customerPaidAmount =  if (paidSubscriber == "yes") {
                              if (random.nextFloat() < 0.8) {
                                utils.randInt(5, 30)
                              } else { // 30 - 99
                                utils.randInt(30, 99)
                              }
                            } else {
                              0
                            }
  val paidDate =  if (customerPaidAmount == 0) {
                    0
                  } else {
                    // generate a date between users registration date and time now
                    dateUtils.genDate(formatter.format(registerDate), formatter.format(Calendar.getInstance().getTimeInMillis))
                  }
  // games played by user based on gender
  val custGamesPlayed = gamesPlayed(custLifeTime, cGender)

  override def toString = custId + " " + custEmail + " " + registerDate + " " + custCountry + " " + custAddress +
                          " " + custLifeTime + " " + custFriendCount + " " + paidSubscriber + " " + customerPaidAmount +
                          " " + paidDate + " " + custGamesPlayed.toString

  private def genEmail = {
    val firstName = cName.split(" ").head
    val lastName = cName.split(" ").last
    val domains = Array("yahoo.com", "gmail.com", "privacy.net", "webmail.com", "msn.com",
      "hotmail.com", "example.com", "privacy.net")
    s"${(firstName + lastName).toLowerCase}${random.nextInt(100)}@${domains(random.nextInt(domains.size))}"
  }

  private def gamesPlayed(customerLifeTime: Int, customerGender: String) = {
    val counter = collection.mutable.Map(
      "city" -> 0,
      "pictionary" -> 0,
      "sniper" -> 0,
      "scramble" -> 0
    )
    val gamesProbMap =  if (customerGender == "female") {
                          Customers.GAMES_FEMALE_PROBABILITY
                        } else {
                          Customers.GAMES_MALE_PROBABILITY
                        }
    1 to customerLifeTime foreach { _ =>
      utils.pickWeightedKey(gamesProbMap) match {
        case "city" => counter("city") += 1
        case "pictionary" => counter("pictionary") += 1
        case "sniper" => counter("sniper") += 1
        case "scramble" => counter("scramble") += 1
      }
    }
    counter
  }
}