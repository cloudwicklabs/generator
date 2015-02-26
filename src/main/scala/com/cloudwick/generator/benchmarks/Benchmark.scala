package com.cloudwick.generator.benchmarks

import com.cloudwick.generator.utils.Utils

/**
 * Benchmark runs (internal only)
 * @author ashrith 
 */
object Benchmark extends App {
  val utils = new Utils

  utils.time("concatenating random ip addresses using mkString()") {
    val random = scala.util.Random
    (1 to 10000).foreach { _ =>
      (random.nextInt(223) + 1) + "." + (1 to 3).map { _ => random.nextInt(255) }.mkString(".")
    }
  }

  utils.time("concatenating random ip addresses using StringBuilder") {
    val random = scala.util.Random
    val sb: StringBuilder = new StringBuilder
    (1 to 10000).foreach { _ =>
      sb.append(random.nextInt(223) + 1)
      (1 to 3).foreach { _ =>
        sb.append(".")
        sb.append(random.nextInt(255))
      }
      sb.toString()
    }
  }
}
