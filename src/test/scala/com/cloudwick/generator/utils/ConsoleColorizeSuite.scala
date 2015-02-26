package com.cloudwick.generator.utils

import org.scalatest.FunSuite

/**
 * Test suite for ConsoleColorize
 * @author ashrith 
 */
class ConsoleColorizeSuite extends FunSuite with ConsoleColorize {
  test("should print text in blue") {
    assert("Blue".blue === "\u001B[34mBlue\u001B[0m")
  }

  test("should print text in black") {
    assert("Black".black === "\u001B[30mBlack\u001B[0m")
  }

  test("should print text in yellow background") {
    assert("YellowBg".yellowBg === "\u001B[43mYellowBg\u001B[0m")
  }

  test("should colorize strings") {
    println("red = " + "red".red)
    println("blue = " + "blue".blue)
    println("yellow = " + "yellow".yellow)
    println("green = " + "green".green)
  }
}
