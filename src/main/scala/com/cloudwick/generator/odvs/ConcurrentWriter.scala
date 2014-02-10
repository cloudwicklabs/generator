package com.cloudwick.generator.odvs

import org.slf4j.LoggerFactory
import java.util.concurrent.{Executors, ExecutorService}
import java.util.concurrent.atomic.AtomicLong
import com.cloudwick.generator.utils.Utils

/**
 * Writer which can handle concurrency
 * @author ashrith 
 */
class ConcurrentWriter(totalEvents: Long, config: OptionsConfig) extends Runnable {
  lazy val logger = LoggerFactory.getLogger(getClass)
  val utils = new Utils
  val threadPool: ExecutorService = Executors.newFixedThreadPool(config.threadPoolSize)
  val finalCounter: AtomicLong = new AtomicLong(0L)
  val finalBytesCounter: AtomicLong = new AtomicLong(0L)
  val messagesPerThread: Int = (totalEvents / config.threadsCount).toInt
  val messagesRange = Range(0, totalEvents.toInt, messagesPerThread)

  def run() = {
    utils.time(s"Generating $totalEvents events") {
      try {
        (1 to config.threadsCount).foreach { threadCount =>
          logger.debug("Initializing thread: {}", threadCount)
          threadPool.execute(
            new Writer(
              messagesRange(threadCount-1),
              messagesRange(threadCount-1) + (messagesPerThread-1),
              finalCounter,
              finalBytesCounter,
              config
            )
          )
        }
      } catch {
        case e: Exception => logger.error("Error: {}", e.printStackTrace())
      } finally {
        threadPool.shutdown()
      }
      while(!threadPool.isTerminated) {
        Thread.sleep(10 * 1000)
        logger.info("Events generated: {}, size: '{}' bytes", finalCounter, finalBytesCounter.longValue())
      }
      logger.info("Total documents processed by {} thread(s): {}", config.threadsCount, finalCounter)
    }
  }
}
