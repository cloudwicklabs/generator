package com.cloudwick.generator.logEvents

import scala.collection.mutable
import scala.util.Random

/**
 * Generates random IP address based on the session information
 * @constructor create new ipGenerator with session count and session length
 * @param sessionCount number of ip(s) in the sessions buffer
 * @param sessionLength total length of the session
 * @author ashrith
 */
class IPGenerator(var sessionCount: Int, var sessionLength: Int) {
  var sessions = mutable.Map[String, Int]()

  /**
   * Picks a random ip from the pool of ip addresses that are in session and then increment's that ip session count
   * @return ip address
   */
  def get_ip: String = {
    sessionGc()
    sessionCreateRec()
    val ip = sessions.keys.toSeq(Random.nextInt(sessions.size))
    sessions(ip) += 1
    ip
  }

  /**
   * Create a new ip session if the sessions count is with in the specified limit, sessionGc() will remove a ip
   * session, if it reaches the specified session length limit and then this method will create a new ip session.
   */
  private def sessionCreate(): Unit = {
    while(sessions.size < sessionCount)
      sessions(randomIp) = 0
  }

  private def sessionCreateRec(): Unit = {
    if (sessions.size < sessionCount) {
      sessions(randomIp) = 0
      sessionCreateRec()
    }
  }

  /**
   * Garbage collect the ip address if the number of times a ip address appeared more than the specified
   * session length
   */
  private def sessionGc(): Unit = {
    sessions.foreach { case(ip, count) =>
      if (count >= sessionLength)
        sessions.remove(ip)
    }
  }

  /**
   * Generates a random ip address, each time this method is called
   * @return ip address
   */
  private def randomIp: String = {
    val random = Random
    (random.nextInt(223) + 1) + "." + (1 to 3).map { _ => random.nextInt(255) }.mkString(".")
  }
}