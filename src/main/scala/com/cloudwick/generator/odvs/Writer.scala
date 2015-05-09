package com.cloudwick.generator.odvs

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import com.cloudwick.generator.avro._
import com.cloudwick.generator.utils.{LazyLogging, AvroFileHandler, FileHandler, Utils}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * Writes events to file
 * @author ashrith 
 */
class Writer(eventsStartRange: Int,
                 eventsEndRange: Int,
                 customersMap: Map[Long, String],
                 counter: AtomicLong,
                 sizeCounter: AtomicLong,
                 config: OptionsConfig) extends Runnable with LazyLogging {
  lazy val utils = new Utils
  val movie = new MovieGenerator

  lazy val sleepTime = if(config.eventsPerSec == 0) 0 else 1000/config.eventsPerSec

  def threadName = Thread.currentThread().getName

  def formatEventToString(odvsEvent: ODVSEvent, formatter: String) = {
    val formatChar = formatter match {
      case "tsv" => '\t'
      case "csv" => ','
      case _ => '\t'
    }
    "%d%c%s%c%d%c%d%c%d%c%s%c%s%c%s%c%s%c%d%c%s\n".format(odvsEvent.cId, formatChar, odvsEvent.cName,
      formatChar, odvsEvent.userActive, formatChar, odvsEvent.cWatchInit, formatChar, odvsEvent.cWatchPauseTime,
      formatChar, odvsEvent.cMovieRating, formatChar, odvsEvent.mId, formatChar, odvsEvent.mName, formatChar,
      odvsEvent.mReleaseDate, formatChar, odvsEvent.mLength, formatChar, odvsEvent.mGenre)
  }

  def formatEventMultiToString(odvsEvent: ODVSEvent, formatter: String) = {
    val formatChar = formatter match {
      case "tsv" => '\t'
      case "csv" => ','
      case _ => '\t'
    }

    Map[String, String](
      "WatchHistory" -> "%d%c%d%c%d%c%s%c%s\n".format(odvsEvent.cId, formatChar, odvsEvent.cWatchInit, formatChar,
        odvsEvent.cWatchPauseTime, formatChar, odvsEvent.mId, formatChar, odvsEvent.mName),
      "CustomerRatings" -> "%d%c%s%c%s%c%s%c%s\n".format(odvsEvent.cId, formatChar, odvsEvent.mId, formatChar,
        odvsEvent.mName, formatChar, odvsEvent.cName, formatChar, odvsEvent.cMovieRating),
      "CustomerQueue" -> "%d%c%d%c%s%c%s%c%s\n".format(odvsEvent.cId, formatChar, odvsEvent.cWatchInit, formatChar,
        odvsEvent.cName, formatChar, odvsEvent.mId, formatChar, odvsEvent.mName),
      "MovieGenre" -> "%s%c%s%c%s%c%d%c%s\n".format(odvsEvent.mGenre, formatChar, odvsEvent.mReleaseDate, formatChar,
        odvsEvent.mId, formatChar, odvsEvent.mLength, formatChar, odvsEvent.mName)
    )
  }

  def avroEvent(odvsEvent: ODVSEvent) = {
    ODVSRecord.newBuilder()
      .setCId(odvsEvent.cId)
      .setCName(odvsEvent.cName)
      .setUserActive(odvsEvent.userActive)
      .setTimeStamp(odvsEvent.cWatchInit)
      .setPauseTime(odvsEvent.cWatchPauseTime)
      .setMovieRating(odvsEvent.cMovieRating)
      .setMovieID(odvsEvent.mId)
      .setMovieName(odvsEvent.mName)
      .setMovieReleaseDate(odvsEvent.mReleaseDate)
      .setMovieLength(odvsEvent.mLength)
      .setMovieGenre(odvsEvent.mGenre)
      .build()
  }

  def watchHistoryEvent(event: ODVSEvent) = {
    WatchHistory.newBuilder()
      .setCId(event.cId)
      .setCustomerTimeWatched(event.cWatchInit)
      .setCustomerTimeWatched(event.cWatchPauseTime)
      .setMovieID(event.mId)
      .setMovieName(event.mName)
      .build()
  }

  def customerRatingEvent(event: ODVSEvent) = {
    CustomerRating.newBuilder()
      .setCId(event.cId)
      .setMovieID(event.mId)
      .setMovieName(event.mName)
      .setCName(event.cName)
      .setMovieRating(event.cMovieRating)
      .build()
  }

  def customerQueueEvent(event: ODVSEvent) = {
    CustomerQueue.newBuilder()
      .setCId(event.cId)
      .setCustomerTimeWatched(event.cWatchInit)
      .setCName(event.cName)
      .setMovieID(event.mId)
      .setMovieName(event.mName)
      .build()
  }

  def movieGenreEvent(event: ODVSEvent) = {
    MovieGenre.newBuilder()
      .setMovieGenre(event.mGenre)
      .setMovieReleaseDate(event.mReleaseDate)
      .setMovieID(event.mId)
      .setMovieLength(event.mLength)
      .setMovieName(event.mName)
      .build()
  }

  def run() = {
    val totalEvents = eventsEndRange - eventsStartRange + 1
    var batchCount: Int = 0
    var outputFileHandler: FileHandler = null
    var outputAvroFileHandler: AvroFileHandler[ODVSRecord] = null
    var outputFileWatchHistoryHandler: FileHandler = null
    var outputFileCustomerRatingsHandler: FileHandler = null
    var outputFileCustomerQueueHandler: FileHandler = null
    var outputFileMovieGenreHandler: FileHandler = null
    var outputAvroFileWatchHistoryHandler: AvroFileHandler[WatchHistory] = null
    var outputAvroFileCustomerRatingsHandler: AvroFileHandler[CustomerRating] = null
    var outputAvroFileCustomerQueueHandler: AvroFileHandler[CustomerQueue] = null
    var outputAvroFileMovieGenreHandler: AvroFileHandler[MovieGenre] = null
    var eventsText: ArrayBuffer[String] = null
    var watchHistoryEventsText: ArrayBuffer[String] = null
    var customerRatingsEventsText: ArrayBuffer[String] = null
    var customerQueueEventsText: ArrayBuffer[String] = null
    var movieGenreEventsText: ArrayBuffer[String] = null
    var multiTableText: Map[String, String] = null
    var eventsAvro: ArrayBuffer[ODVSRecord] = null
    var watchHistoryEventsAvro: ArrayBuffer[WatchHistory] = null
    var customerRatingsEventsAvro: ArrayBuffer[CustomerRating] = null
    var customerQueueEventsAvro: ArrayBuffer[CustomerQueue] = null
    var movieGenreEventsAvro: ArrayBuffer[MovieGenre] = null
    var watchHistory: WatchHistory = null
    var customerRating: CustomerRating = null
    var customerQueue: CustomerQueue = null
    var odvsEvent: ODVSEvent = null
    var movieGenre: MovieGenre = null
    var textPlaceHolder:String = null
    var avroPlaceHolder: ODVSRecord = null

    if (config.multiTable) {
      if (config.outputFormat == "avro") {
        outputAvroFileWatchHistoryHandler = new AvroFileHandler[WatchHistory](new File(config.filePath, s"odvs_watch_history_$threadName.data").toString, config.fileRollSize)
        outputAvroFileCustomerRatingsHandler = new AvroFileHandler[CustomerRating](new File(config.filePath, s"odvs_customer_rating_$threadName.data").toString, config.fileRollSize)
        outputAvroFileCustomerQueueHandler = new AvroFileHandler[CustomerQueue](new File(config.filePath, s"odvs_customer_queue_$threadName.data").toString, config.fileRollSize)
        outputAvroFileMovieGenreHandler = new AvroFileHandler[MovieGenre](new File(config.filePath, s"odvs_movie_genre_$threadName.data").toString, config.fileRollSize)
        watchHistoryEventsAvro = new ArrayBuffer[WatchHistory](config.flushBatch)
        customerRatingsEventsAvro = new ArrayBuffer[CustomerRating](config.flushBatch)
        customerQueueEventsAvro = new ArrayBuffer[CustomerQueue](config.flushBatch)
        movieGenreEventsAvro = new ArrayBuffer[MovieGenre](config.flushBatch)
      } else {
        outputFileWatchHistoryHandler = new FileHandler(new File(config.filePath, s"odvs_watch_history_$threadName.data").toString, config.fileRollSize)
        outputFileCustomerRatingsHandler = new FileHandler(new File(config.filePath, s"odvs_customer_rating_$threadName.data").toString, config.fileRollSize)
        outputFileCustomerQueueHandler = new FileHandler(new File(config.filePath, s"odvs_customer_queue_$threadName.data").toString, config.fileRollSize)
        outputFileMovieGenreHandler = new FileHandler(new File(config.filePath, s"odvs_movie_genre_$threadName.data").toString, config.fileRollSize)
        watchHistoryEventsText  = new ArrayBuffer[String](config.flushBatch)
        customerRatingsEventsText  = new ArrayBuffer[String](config.flushBatch)
        customerQueueEventsText  = new ArrayBuffer[String](config.flushBatch)
        movieGenreEventsText  = new ArrayBuffer[String](config.flushBatch)
      }
    } else {
      if (config.outputFormat == "avro") {
        outputAvroFileHandler = new AvroFileHandler[ODVSRecord](new File(config.filePath,s"odvs_$threadName.data").toString, config.fileRollSize)
        eventsAvro = new ArrayBuffer[ODVSRecord](config.flushBatch)
      } else {
        outputFileHandler = new FileHandler(new File(config.filePath, s"odvs_$threadName.data").toString, config.fileRollSize)
        eventsText  = new ArrayBuffer[String](config.flushBatch)
      }
    }

    try {
      if (config.outputFormat == "avro") {
        if (config.multiTable) {
          outputAvroFileWatchHistoryHandler.openFile()
          outputAvroFileCustomerRatingsHandler.openFile()
          outputAvroFileCustomerQueueHandler.openFile()
          outputAvroFileMovieGenreHandler.openFile()
        } else {
          outputAvroFileHandler.openFile()
        }
      } else {
        if (config.multiTable) {
          outputFileWatchHistoryHandler.openFile()
          outputFileCustomerRatingsHandler.openFile()
          outputFileCustomerQueueHandler.openFile()
          outputFileMovieGenreHandler.openFile()
        } else {
          outputFileHandler.openFile()
        }
      }

      (eventsStartRange to eventsEndRange).foreach { eventCount =>
        batchCount += 1
        odvsEvent = new ODVSGenerator(customersMap, movie).eventGenerate

        /*
         * Fill the buffers
         */
        if (config.multiTable) {
          if (config.outputFormat == "avro") {
            watchHistory = watchHistoryEvent(odvsEvent)
            customerRating = customerRatingEvent(odvsEvent)
            customerQueue = customerQueueEvent(odvsEvent)
            movieGenre = movieGenreEvent(odvsEvent)

            watchHistoryEventsAvro += watchHistory
            sizeCounter.getAndAdd(watchHistory.toString.getBytes.length)
            customerRatingsEventsAvro += customerRating
            sizeCounter.getAndAdd(customerRating.toString.getBytes.length)
            customerQueueEventsAvro += customerQueue
            sizeCounter.getAndAdd(customerQueue.toString.getBytes.length)
            movieGenreEventsAvro += movieGenre
            sizeCounter.getAndAdd(movieGenre.toString.getBytes.length)
          } else {
            multiTableText = formatEventMultiToString(odvsEvent, config.outputFormat)
            watchHistoryEventsText += multiTableText("WatchHistory")
            sizeCounter.getAndAdd(multiTableText("WatchHistory").getBytes.length)
            customerRatingsEventsText += multiTableText("CustomerRatings")
            sizeCounter.getAndAdd(multiTableText("CustomerRatings").getBytes.length)
            customerQueueEventsText += multiTableText("CustomerQueue")
            sizeCounter.getAndAdd(multiTableText("CustomerQueue").getBytes.length)
            movieGenreEventsText += multiTableText("MovieGenre")
            sizeCounter.getAndAdd(multiTableText("MovieGenre").getBytes.length)
          }
        } else {
          if (config.outputFormat == "avro") {
            avroPlaceHolder = avroEvent(odvsEvent)
            eventsAvro += avroPlaceHolder
            sizeCounter.getAndAdd(avroPlaceHolder.toString.getBytes.length)
          } else {
            textPlaceHolder = formatEventToString(odvsEvent, config.outputFormat)
            eventsText += textPlaceHolder
            sizeCounter.getAndAdd(textPlaceHolder.getBytes.length)
          }
        }
        // increment universal record counter
        counter.getAndIncrement
        if (batchCount == config.flushBatch || batchCount == totalEvents) {
          if (config.multiTable) {
            if (config.outputFormat == "avro") {
              outputAvroFileWatchHistoryHandler.publishBuffered(watchHistoryEventsAvro)
              outputAvroFileCustomerQueueHandler.publishBuffered(customerQueueEventsAvro)
              outputAvroFileCustomerRatingsHandler.publishBuffered(customerRatingsEventsAvro)
              outputAvroFileMovieGenreHandler.publishBuffered(movieGenreEventsAvro)
              watchHistoryEventsAvro.clear()
              customerQueueEventsAvro.clear()
              customerQueueEventsAvro.clear()
              movieGenreEventsAvro.clear()
            } else {
              outputFileWatchHistoryHandler.publishBuffered(watchHistoryEventsText)
              outputFileCustomerQueueHandler.publishBuffered(customerQueueEventsText)
              outputFileCustomerRatingsHandler.publishBuffered(customerRatingsEventsText)
              outputFileMovieGenreHandler.publishBuffered(movieGenreEventsText)
              watchHistoryEventsText.clear()
              customerQueueEventsText.clear()
              customerRatingsEventsText.clear()
              movieGenreEventsText.clear()
            }
            batchCount = 0
          } else {
            if (config.outputFormat == "avro") {
              outputAvroFileHandler.publishBuffered(eventsAvro)
              eventsAvro.clear()
            } else {
              outputFileHandler.publishBuffered(eventsText)
              eventsText.clear()
            }
            batchCount = 0
          }
        }
      }
      logger.debug(s"Events generated by $threadName is: $totalEvents from ($eventsStartRange) to ($eventsEndRange)")
    } catch {
      case e: Exception => logger.error("Error:: {}", e)
    }
    finally {
      if (config.multiTable) {
        if (config.outputFormat == "avro") {
          outputAvroFileWatchHistoryHandler.close()
          outputAvroFileCustomerQueueHandler.close()
          outputAvroFileCustomerRatingsHandler.close()
          outputAvroFileMovieGenreHandler.close()
        } else {
          outputFileWatchHistoryHandler.close()
          outputFileCustomerQueueHandler.close()
          outputFileCustomerRatingsHandler.close()
          outputFileMovieGenreHandler.close()
        }
      } else {
        if (config.outputFormat == "avro") {
          outputAvroFileHandler.close()
        } else {
          outputFileHandler.close()
        }
      }
    }
  }
}
