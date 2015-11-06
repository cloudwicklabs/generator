package com.cloudwick.generator.manufacturer

/**
 * Class for wrapping default command line options
 * @author ashrith
 */
case class OptionsConfig(
  eventsPerSec: Int = 1,
  outputFormat: String = "tsv",
  filePath: String = "/tmp",
  fileRollSize: Int = Int.MaxValue, // in bytes
  totalEvents: Long = 1000,
  flushBatch: Int = 10000,
  minEntriesPerManufacturer: Int = 10,
  maxEntriesPerManufacturer: Int = 25,
  minManufacturerCombinations: Int = 1,
  maxManufacturerCombinations: Int = 5,
  threadsCount: Int = 1,
  threadPoolSize: Int = 10,
  logLevel: String = "INFO"
)
