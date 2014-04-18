package com.cloudwick.generator.logEvents

import org.slf4j.LoggerFactory

/**
 * Log events generator driver
 * @author ashrith 
 */
object Driver extends App {
  private val logger = LoggerFactory.getLogger(getClass)

  /*
   * Command line option parser
   */
  val optionsParser = new scopt.OptionParser[OptionsConfig]("generator") {
    head("Log Generator")
    opt[Int]("eventsPerSec") action { (x, c) =>
      c.copy(eventsPerSec = x)
    } text "number of log events to generate per sec, use this to throttle the generator"
    opt[String]('o', "outputFormat") action { (x, c) =>
      c.copy(outputFormat = x)
    } validate { x: String =>
      if (x == "text" || x == "avro" || x == "seq")
        success
      else
        failure("supported output format's: string, avro, seq'")
    } text "format of the string to write to the file defaults to: 'tsv'\n" +
      "\t where,\n" +
      "\t\ttext - string formatted by tabs in between columns\n" +
      "\t\tavro - string formatted using avro serialization\n"
      //"\t\tseq - string formatted using sequence serialization"
    opt[String]('d', "destination") action { (x, c) =>
      c.copy(destination = x)
    } validate { x: String =>
      if (x == "file" || x == "kafka")
        success
      else
        failure("supported destination formats are: file, kafka")
    } text "destination where the generator writes data to, defaults to: 'file'"
    opt[Int]('s', "fileRollSize") action { (x, c) =>
      c.copy(fileRollSize = x)
    } text "size of the file to roll in bytes, defaults to: Int.MaxValue (don't roll files)"
    opt[String]('p', "filePath") action { (x, c) =>
      c.copy(filePath = x)
    } text "path of the file where the data should be generated, defaults to: '/tmp'"
    opt[String]('k', "kafkaBrokerList") action { (x, c) =>
      c.copy(kafkaBrokerList = x)
    } text "list of kafka brokers to write to, defaults to: 'localhost:9092'"
    opt[String]('q', "kafkaTopicName") action { (x, c) =>
      c.copy(kafkaTopicName = x)
    } text "name of the kafka topic to write data to, defaults to: 'logs'"
    opt[Long]('e', "totalEvents") action { (x, c) =>
      c.copy(totalEvents = x)
    } text "total number of events to generate, default: 1000"
    opt[Int]('b', "flushBatch") action { (x, c) =>
      c.copy(flushBatch = x)
    } text "number of events to flush to file at a single time, defaults to: 10000"
    opt[Int]('s', "ipSessionCount") action { (x, c) =>
      c.copy(ipSessionCount = x)
    } text "number of times a ip can appear in a session, defaults to: '25'"
    opt[Int]('l', "ipSessionLength") action { (x, c) =>
      c.copy(ipSessionLength = x)
    } text "size of the session, defaults to: '50'"
    opt[Int]('t', "threadsCount") action { (x, c) =>
      c.copy(threadsCount = x)
    } text "number of threads to use for write and read operations, defaults to: 1"
    opt[Int]('p', "threadPoolSize") action { (x, c) =>
      c.copy(threadPoolSize = x)
    } text "size of the thread pool, defaults to: 10"
    help("help") text "prints this usage text"
  }

  optionsParser.parse(args, OptionsConfig()) map { config =>
    logger.info(s"Successfully parsed command line args")
    config.getClass.getDeclaredFields.map(_.getName).zip( config.productIterator.to ).toMap.foreach { configElements =>
      logger.info("Configuration element '{}' = '{}'", configElements._1, configElements._2)
    }
    try {
      logger.info("Initializing generator ...")
      new ConcurrentWriter(config.totalEvents, config).run()
    } catch {
      case e: Exception => logger.error("Error : {}", e.fillInStackTrace())
    }
  } getOrElse {
    logger.error("Failed to parse command line arguments")
  }
}
