package com.cloudwick.generator.logEvents

import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.cloudwick.generator.utils.LazyLogging

/**
 * Log events generator driver
 * @author ashrith 
 */
object Driver extends App with LazyLogging {

  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]

  /*
   * Command line option parser
   */
  val optionsParser = new scopt.OptionParser[OptionsConfig]("generator") {
    head("Log Generator")
    opt[Int]('e', "eventsPerSec") action { (x, c) =>
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
      "\t\tavro - string formatted using avro serialization"
      //"\t\tseq - string formatted using sequence serialization"
    opt[String]('d', "destination") action { (x, c) =>
      c.copy(destination = x)
    } validate { x: String =>
      if (x == "file" || x == "kafka" || x == "kinesis")
        success
      else
        failure("supported destination formats are: 'file', 'kafka' or 'kinesis'")
    } text "destination where the generator writes data to, defaults to: 'file'\n" +
      "\t where,\n" +
      "\t\tfile - output's directly to flat files\n" +
      "\t\tkafka - output to specified kafka topic\n" +
      "\t\tkinesis - output to specified kinesis"
    opt[Int]('r', "fileRollSize") action { (x, c) =>
      c.copy(fileRollSize = x)
    } text "size of the file to roll in bytes, defaults to: Int.MaxValue (~2GB)"
    opt[String]('p', "filePath") action { (x, c) =>
      c.copy(filePath = x)
    } text "path of the file where the data should be generated, defaults to: '/tmp'"
    opt[Long]('t', "totalEvents") action { (x, c) =>
      c.copy(totalEvents = x)
    } text "total number of events to generate, default: 1000"
    opt[Int]('b', "flushBatch") action { (x, c) =>
      c.copy(flushBatch = x)
    } text "number of events to flush to file at a single time, defaults to: 10000"
    opt[String]("kafkaBrokerList") action { (x, c) =>
      c.copy(kafkaBrokerList = x)
    } text "list of kafka brokers to write to, defaults to: 'localhost:9092'"
    opt[String]("kafkaTopicName") action { (x, c) =>
      c.copy(kafkaTopicName = x)
    } text "name of the kafka topic to write data to, defaults to: 'logs'"
    opt[String]("kinesisStreamName") action { (x, c) =>
      c.copy(kinesisStreamName = x)
    } text "name of the kinesis stream to write data to, defaults to: 'logevents'"
    opt[Int]("kinesisShardCount") action { (x, c) =>
      c.copy(kinesisShardCount = x)
    } text "number of kinesis shard to create, defaults to: '1'"
    opt[Int]("ipSessionCount") action { (x, c) =>
      c.copy(ipSessionCount = x)
    } text "number of times a ip can appear in a session, defaults to: '25'"
    opt[Int]("ipSessionLength") action { (x, c) =>
      c.copy(ipSessionLength = x)
    } text "size of the session, defaults to: '50'"
    opt[Int]("threadsCount") action { (x, c) =>
      c.copy(threadsCount = x)
    } text "number of threads to use for write and read operations, defaults to: 1"
    opt[Int]("threadPoolSize") action { (x, c) =>
      c.copy(threadPoolSize = x)
    } text "size of the thread pool, defaults to: 10"
    opt[String]("awsAccessKey") action { (x, c) =>
      c.copy(awsAccessKey = x)
    } text "AWS access key (required for kinesis)"
    opt[String]("awsSecretKey") action { (x, c) =>
      c.copy(awsSecretKey = x)
    } text "AWS secret key (required for kinesis)"
    opt[String]("awsEndPoint") action { (x, c) =>
      c.copy(awsEndPoint = x)
    } text "AWS service end point to connect to (required for kinesis)"
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

    if (config.destination == "kinesis") {
      if (config.awsAccessKey.isEmpty || config.awsSecretKey.isEmpty || config.awsEndPoint.isEmpty) {
        logger.error("Missing 'awsAccessKey', 'awsSecretKey' and 'awsEndPoint' arguments")
        System.exit(1)
      }
    }

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
