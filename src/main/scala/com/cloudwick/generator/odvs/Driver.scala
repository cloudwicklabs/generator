package com.cloudwick.generator.odvs

import ch.qos.logback.classic.{Level, Logger}
import com.cloudwick.generator.utils.LazyLogging
import org.slf4j.LoggerFactory

/**
 * OnDemand video service generator driver
 * @author ashrith
 */
object Driver extends App with LazyLogging {
  private val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]

  /*
   * Command line option parser
   */
  val optionsParser = new scopt.OptionParser[OptionsConfig]("generator") {
    head("OnDemand Video Service Analytics")
    opt[Int]('e', "eventsPerSec") action { (x, c) =>
      c.copy(eventsPerSec = x)
    } text "number of events to generate per sec, use this to throttle the generator"
    opt[String]('o', "outputFormat") action { (x, c) =>
      c.copy(outputFormat = x)
    } validate { x: String =>
      if (x == "tsv" || x == "csv" || x == "avro" || x == "seq")
        success
      else
        failure("supported file format's: csv, tsv, avro, seq'")
    } text "format of the string to write to the file defaults to: 'tsv'\n" +
      "\t where,\n" +
      "\t\ttsv - string formatted by tabs in between columns\n" +
      "\t\tcsv - string formatted by commas in between columns\n" +
      "\t\tavro - string formatted using avro serialization\n"
      //"\t\tseq - string formatted using sequence serialization"
    opt[Int]('r', "fileRollSize") action { (x, c) =>
      c.copy(fileRollSize = x)
    } text "size of the file to rollin bytes, defaults to: Int.MaxValue (~2GB)"
    opt[String]('p', "filePath") action { (x, c) =>
      c.copy(filePath = x)
    } text "path of the file where the data should be generated, defaults to: '/tmp'"
    opt[Long]('t', "totalEvents") action { (x, c) =>
      c.copy(totalEvents = x)
    } text "total number of events to generate, default: 1000"
    opt[Long]('c', "customersDataSetSize") action { (x, c) =>
      c.copy(customerDataSetSize = x)
    } text "total number of customers data set to build, default: 100000L"
    opt[Unit]('m', "multiTable") action { (_, c) =>
      c.copy(multiTable = true)
    } text "generate data as multi tables format, default: false"
    opt[Unit]('d', "dumpCustomers") action { (x, c) =>
      c.copy(dumpCustomers =  true)
    } text "whether to dump customers to a file"
    opt[Int]('b', "flushBatch") action { (x, c) =>
      c.copy(flushBatch = x)
    } text "number of events to flush to file at a single time, defaults to: 10000"
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
    // Set logging level
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
      new ConcurrentWriter(config.totalEvents, config).run()
    } catch {
      case e: Exception => logger.error("Error : {}", e.fillInStackTrace())
    }
  } getOrElse {
    logger.error("Failed to parse command line arguments")
  }
}
