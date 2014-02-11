package com.cloudwick.generator.osge

/**
 * Class for wrapping default command line options
 * @author ashrith
 */
case class OptionsConfig(
  eventsPerSec: Int = 1,
  fileFormat: String = "tsv",
  filePath: String = "/tmp",
  fileRollSize: Int = Int.MaxValue, // in bytes
  totalEvents: Long = 1000,
  flushBatch: Int = 10000,
  multiTable: Boolean = false,
  threadsCount: Int = 1,
  threadPoolSize: Int = 10
)