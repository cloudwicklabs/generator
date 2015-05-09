package com.cloudwick.generator.odvs

import java.io.File

import org.slf4j.LoggerFactory
import java.util.concurrent.{Executors, ExecutorService}
import java.util.concurrent.atomic.AtomicLong
import com.cloudwick.generator.utils.{LazyLogging, FileHandler, Utils}

/**
 * Writer which can handle concurrency
 * @author ashrith 
 */
class ConcurrentWriter(totalEvents: Long, config: OptionsConfig) extends Runnable with LazyLogging {
  val utils = new Utils
  val threadPool: ExecutorService = Executors.newFixedThreadPool(config.threadPoolSize)
  val finalCounter: AtomicLong = new AtomicLong(0L)
  val finalBytesCounter: AtomicLong = new AtomicLong(0L)
  val messagesPerThread: Int = (totalEvents / config.threadsCount).toInt
  val messagesRange = Range(0, totalEvents.toInt, messagesPerThread)
  val customers = scala.collection.mutable.Map[Long, String]()
  val person = new Person

  def buildCustomersMap = {
    logger.info("Building a customer data set of size: {}", config.customerDataSetSize)
    (1L to config.customerDataSetSize).foreach { custId =>
      customers += custId -> person.gen
    }
    customers.toMap
  }

  def writeCustomersMap (customersMap: Map[Long, String]) = {
    val formatChar = config.outputFormat match {
      case "tsv" => '\t'
      case "csv" => ','
      case _ => '\t'
    }
    var outputFileWatchHistoryHandler: FileHandler = null
    try {
      outputFileWatchHistoryHandler = new FileHandler(new File(config.filePath, "odvs_customers.data").toString, config.fileRollSize)
      customersMap.foreach { cm =>
        outputFileWatchHistoryHandler.publish("%d%c%s\n".format(cm._1, formatChar, cm._2))
      }
    } finally {
      outputFileWatchHistoryHandler.close()
    }
  }

  def run() = {
    utils.time(s"Generating $totalEvents events") {
      val customersData: Map[Long, String] = buildCustomersMap
      try {
        (1 to config.threadsCount).foreach { threadCount =>
          logger.debug("Initializing thread: {}", threadCount)
          threadPool.execute(
            new Writer(
              messagesRange(threadCount-1),
              messagesRange(threadCount-1) + (messagesPerThread-1),
              customersData,
              finalCounter,
              finalBytesCounter,
              config
            )
          )
        }
      } catch {
        case e: Exception => logger.error("Error:: {}", e.printStackTrace())
      } finally {
        threadPool.shutdown()
      }
      while(!threadPool.isTerminated) {
        logger.trace("Sleeping for 10 seconds ...")
        Thread.sleep(10 * 1000)
        logger.info("Events generated: {}, size: '{}' bytes", finalCounter, finalBytesCounter.longValue())
      }
      if (config.dumpCustomers) {
        writeCustomersMap(customersData)
      }
      logger.info("Total documents processed by {} thread(s): {}", config.threadsCount, finalCounter)
    }
  }
}
