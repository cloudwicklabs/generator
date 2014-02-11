package com.cloudwick.generator.osge

import scala.util.Random

/**
 * Description goes here
 * @author ashrith 
 */
class Address {
  private val random = Random

  private val streetNames = Array(
    "Acacia", "Beech", "Birch", "Cedar", "Cherry", "Chestnut", "Elm", "Larch", "Laurel",
    "Linden", "Maple", "Oak", "Pine", "Rose", "Walnut", "Willow", "Adams", "Franklin", "Jackson", "Jefferson",
    "Lincoln", "Madison", "Washington", "Wilson", "Churchill", "Tyndale", "Latimer", "Cranmer", "Highland",
    "Hill", "Park", "Woodland", "Sunset", "Virginia", "1st", "2nd", "4th", "5th", "34th", "42nd"
  )

  private val streetTypes = Array(
    "St", "Ave", "Rd", "Blvd", "Trl", "Rdg", "Pl", "Pkwy", "Ct", "Circle"
  )

  private val line2Types = Array(
    "Apt", "Bsmt", "Bldg", "Dept", "Fl", "Frnt", "Hngr", "Lbby", "Lot", "Lowr", "Ofc", "Ph", "Pier", "Rear", "Rm",
     "Side", "Slip", "Spc", "Stop", "Ste", "Trlr", "Unit", "Uppr"
  )

  private val usStates = Array(
    "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA",
    "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR",
    "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"
  )

  /**
   * Generates a address
   * @return a name
   */
  def gen: String = {
    addressLine1 + " " + addressLine2 + " " + state + " " + zipCode
  }

  override def toString = addressLine1 + " " + addressLine2 + " " + state + " " + zipCode

  private def addressLine1 = s"${random.nextInt(4000)} ${streetNames(random.nextInt(streetNames.size))} ${streetTypes(random.nextInt(streetTypes.size))}"

  private def addressLine2 = s"${line2Types(random.nextInt(line2Types.size))} ${random.nextInt(999)}"

  private def zipCode = "%05d".format(random.nextInt(99999))

  private def state = usStates(random.nextInt(usStates.size))
}
