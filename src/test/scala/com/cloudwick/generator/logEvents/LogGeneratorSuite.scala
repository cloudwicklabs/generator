package com.cloudwick.generator.logEvents

import org.scalatest.FunSuite

/**
 * Test Suite for Log Generator
 * @author ashrith 
 */
class LogGeneratorSuite extends FunSuite {
  val logEventRegex = """([\d.]+) (\S+) (\S+) \[(.*)\] "([^\s]+) (/[^\s]*) HTTP/[^\s]+" (\d{3}) (\d+) "([^"]+)" "([^"]+)"""".r
  val logGenerator = new LogGenerator(new IPGenerator(5, 25))

  val testEvent = new LogEvent("0.0.0.0", "00/Jan/1800:00:00:00 -0700", "/test.php", 200, 1000,
    "Mozilla/5.0 (X11; Linux x86_64; rv:6.0a1) Gecko/20110421 Firefox/6.0a1")

  test("simple log event should match the regex") {
    assert(logEventRegex.findFirstIn(testEvent.toString) != None)
  }

  test("log event's should match regex pattern") {
    (1 to 25).foreach { _ =>
      assert(logEventRegex.findFirstIn(logGenerator.eventGenerate.toString) != None)
    }
  }
}
