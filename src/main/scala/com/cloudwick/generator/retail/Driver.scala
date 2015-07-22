package com.cloudwick.generator.retail

import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.cloudwick.generator.utils.LazyLogging

/**
 * Retail data generator driver
 */
object Driver extends App with LazyLogging {

  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]

  /*
   * Command line option parser
   */
  val optionsParser = new scopt.OptionParser[OptionsConfig]("generator") {
    head("Retail Generator")
    opt[Int]('e', "eventsPerSec") action { (x, c) =>
      c.copy(eventsPerSec = x)
    } text "number of log events to generate per sec, use this to throttle the generator"
    opt[String]('o', "outputFormat") action { (x, c) =>
      c.copy(outputFormat = x)
    } validate { x: String =>
      if (x == "text")
        success
      else
        failure("supported output format's: string'")
    } text "format of the string to write to the file defaults to: 'tsv'\n" +
      "\t where,\n" +
      "\t\ttext - string formatted by tabs in between columns\n"
    opt[String]('d', "destination") action { (x, c) =>
      c.copy(destination = x)
    } validate { x: String =>
      if (x == "file")
        success
      else
        failure("supported destination formats are: 'file'")
    } text "destination where the generator writes data to, defaults to: 'file'\n" +
      "\t where,\n" +
      "\t\tfile - output's directly to flat files\n"
    opt[Int]('r', "fileRollSize") action { (x, c) =>
      c.copy(fileRollSize = x)
    } text "size of the file to roll in bytes, defaults to: Int.MaxValue (~2GB)"
    opt[String]('p', "filePath") action { (x, c) =>
      c.copy(filePath = x)
    } text "path of the file where the data should be generated, defaults to: '/tmp'"
    opt[Int]('b', "flushBatch") action { (x, c) =>
      c.copy(flushBatch = x)
    } text "number of events to flush to file at a single time, defaults to: 10000"
    opt[Int]("storesCount") action { (x, c) =>
      c.copy(storesCount = x)
    } text "number of stores to be used, defaults to: '10'"
    opt[Int]("productsCount") action { (x, c) =>
      c.copy(productsCount = x)
    } text "number of products with in each store id, defaults to: '100'"
    opt[String]("startDate") action { (x, c) =>
      c.copy(startDate = x)
    } text "start date to be used for generating date, defaults to: today (yyyy-MM-dd)"
    opt[String]("endDate") action { (x, c) =>
      c.copy(endDate = x)
    } text "end date to be used for generating date, defaults to: today + 10 (yyyy-MM-dd)"
    opt[Int]("threadsCount") action { (x, c) =>
      c.copy(threadsCount = x)
    } text "number of threads to use for write and read operations, defaults to: 1"
    opt[Int]("threadPoolSize") action { (x, c) =>
      c.copy(threadPoolSize = x)
    } text "size of the thread pool, defaults to: 10"
    opt[String]("loggingLevel") action { (x, c) =>
      c.copy(logLevel = x)
    } text "Logging level to set, defaults to: INFO"
    help("help") text "prints this usage text"
  }

  optionsParser.parse(args, OptionsConfig()) map { config =>
    // Set the logging level
    val logLevel = config.logLevel match {
      case "INFO" |"info"   => Level.INFO
      case "TRACE"|"trace"  => Level.TRACE
      case "DEBUG"|"debug"  => Level.DEBUG
      case "WARN" |"warn"   => Level.WARN
      case "ERROR"|"error"  => Level.ERROR
    }
    root.setLevel(logLevel)

    logger.info(s"Successfully parsed command line args")
    config
      .getClass
      .getDeclaredFields
      .map(_.getName)
      .zip(config.productIterator.to)
      .toMap
      .foreach { configElements =>
      logger.info("Configuration element '{}' = '{}'", configElements._1, configElements._2)
    }
    try {
      logger.info("Initializing generator ...")
      new ConcurrentWriter(config).run()
    } catch {
      case e: Exception => logger.error("Error : {}", e.fillInStackTrace())
    }
  } getOrElse {
    logger.error("Failed to parse command line arguments")
  }
}
