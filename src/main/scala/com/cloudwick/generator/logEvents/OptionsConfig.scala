package com.cloudwick.generator.logEvents

/**
 * Class for wrapping default command line options
 * @author ashrith
 */
case class OptionsConfig(
  awsAccessKey: String = "",
  awsSecretKey: String = "",
  awsEndPoint: String = "",
  eventsPerSec: Int = 0,
  destination: String = "file",
  kafkaBrokerList: String = "localhost:9092",
  kafkaTopicName: String = "logs",
  kinesisStreamName: String = "generator",
  kinesisShardCount: Int = 1,
  outputFormat: String = "string",
  filePath: String = "/tmp",
  fileRollSize: Int = Int.MaxValue, // in bytes
  totalEvents: Long = 1000,
  flushBatch: Int = 10000,
  ipSessionCount: Int = 25,
  ipSessionLength: Int = 50,
  threadsCount: Int = 1,
  threadPoolSize: Int = 10,
  logLevel: String = "INFO"
)