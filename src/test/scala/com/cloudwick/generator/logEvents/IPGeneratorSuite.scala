package com.cloudwick.generator.logEvents

import org.scalatest.FunSuite

import scala.collection.mutable.ListBuffer

/**
 * Test Suite for IPGenerator
 * @author ashrith 
 */
class IPGeneratorSuite extends FunSuite {
  val totalUsersInSession = 5
  val maxSessionCountPerIp = 25
  val ipGen = new IPGenerator(totalUsersInSession, maxSessionCountPerIp)
  val ipPattern = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$"

  test("ip regex pattern should work with 0.0.0.0") {
    assert("0.0.0.0".matches(ipPattern))
  }

  test("ip regex pattern should not work with 192.168.1.260") {
    assert(!"192.168.1.260".matches(ipPattern))
  }

  test("ip regex pattern should not work with 0.0.0") {
    assert(!"0.0.0".matches(ipPattern))
  }

  test("calling get_ip should return a proper ip") {
    (1 to 10).foreach(_ => assert(ipGen.get_ip.matches(ipPattern)))
  }

  test("make sure ip does not show up in a session after its respective session expires") {
    // in this case each ip address cannot be greater than 25 occurrences
    val ipLGen = new IPGenerator(totalUsersInSession, maxSessionCountPerIp)
    val ips = ListBuffer[String]()
    (1 to 50).foreach(_ => ips += ipLGen.get_ip)
    ips.groupBy(l => l).map(t => (t._1, t._2.length)).foreach {
      case(k, v) =>
        println("\t ip: " + k + ", session_count: " + v)
        assert(v.toInt < maxSessionCountPerIp)
    }
  }
}
