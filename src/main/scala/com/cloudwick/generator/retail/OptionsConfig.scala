package com.cloudwick.generator.retail

import com.cloudwick.generator.utils.DateUtils

case class OptionsConfig(
  eventsPerSec: Int = 0,
  destination: String = "file",
  outputFormat: String = "string",
  filePath: String = "/tmp",
  fileRollSize: Int = Int.MaxValue, // in bytes
  totalEvents: Long = 1000,
  flushBatch: Int = 10000,
  storesCount: Int = 10,
  productsCount: Int = 100,
  startDate: String = new DateUtils().getCurrentDate,
  endDate: String = new DateUtils().addDays(new DateUtils().getCurrentDate, 10),
  threadsCount: Int = 1,
  threadPoolSize: Int = 10,
  logLevel: String = "INFO"
)