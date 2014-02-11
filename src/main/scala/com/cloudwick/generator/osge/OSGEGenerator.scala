package com.cloudwick.generator.osge

/**
 * Description goes here
 * @author ashrith 
 */
class OSGEGenerator {

  def eventGenerate = {
    val person = new Person
    val customerID = java.util.UUID.randomUUID.toString
    val customer = new Customers(customerID, person.name, person.gender)

    new OSGEEvent(
      customer.custId,
      customer.custName,
      customer.custEmail,
      person.gender,
      person.age,
      customer.custAddress,
      customer.custCountry,
      customer.registerDate,
      customer.custFriendCount,
      customer.custLifeTime,
      customer.custGamesPlayed("city"),
      customer.custGamesPlayed("pictionary"),
      customer.custGamesPlayed("scramble"),
      customer.custGamesPlayed("sniper"),
      customer.customerPaidAmount,
      customer.paidSubscriber,
      customer.paidDate
    )
  }
}
