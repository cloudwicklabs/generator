package com.cloudwick.generator.odvs

import scala.util.Random

/**
 * Description goes here
 * @author ashrith 
 */
class ODVSGenerator(customersMap: Map[Long, String], movieGenerator: MovieGenerator) {
  private val customersSize = customersMap.size

  def eventGenerate = {
    val custID = Random.nextInt(customersSize) + 1
    val custName = customersMap(custID)
    val movieInfo: Array[String] = movieGenerator.gen
    val customer = new Customers(custID, custName, movieInfo(3).toInt)

    new ODVSEvent(
      custID,
      custName,
      customer.userActiveOrNot.toInt,
      customer.timeWatched,
      customer.pausedTime,
      customer.rating,
      movieInfo(0), //movieId
      movieInfo(1).replace("'", ""), //movieName
      movieInfo(2), //movieReleaseDate
      movieInfo(3).toInt, //movieRunTime
      movieInfo(4) //movieGenre
    )
  }
}
