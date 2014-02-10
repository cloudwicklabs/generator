package com.cloudwick.generator.logEvents

/**
 * Class for wrapping default command line options
 * @author ashrith
 */
case class OptionsConfig(
  eventsPerSec: Int = 1,
  fileFormat: String = "string",
  filePath: String = "/tmp",
  fileRollSize: Int = Int.MaxValue, // in bytes
  totalEvents: Long = 1000,
  flushBatch: Int = 10000,
  ipSessionCount: Int = 25,
  ipSessionLength: Int = 50,
  threadsCount: Int = 1,
  threadPoolSize: Int = 10
)