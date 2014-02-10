package com.cloudwick.generator.odvs

import java.util.concurrent.atomic.AtomicLong
import org.slf4j.LoggerFactory
import com.cloudwick.generator.utils.{AvroFileHandler, FileHandler, Utils}
import scala.collection.mutable.ArrayBuffer
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.Schema
import java.io.File

/**
 * Writes events to file
 * @author ashrith 
 */
class Writer(eventsStartRange: Int,
                 eventsEndRange: Int,
                 customersMap: Map[Long, String],
                 counter: AtomicLong,
                 sizeCounter: AtomicLong,
                 config: OptionsConfig) extends Runnable {
  lazy val logger = LoggerFactory.getLogger(getClass)
  lazy val utils = new Utils
  lazy val fileUtils = new utils.FileUtils
  val movie = new MovieGenerator

  lazy val schemaLine =
    """
      |{
      | "type":"record",
      | "name":"ODVS",
      | "fields":[
      |   {"name":"CId","type":"int"},
      |   {"name":"CName","type":"string"},
      |   {"name":"UserActive","type":"int"},
      |   {"name":"TimeStamp","type":"long"},
      |   {"name":"PauseTime","type":"long"},
      |   {"name":"MovieRating","type":"string"},
      |   {"name":"MovieID","type":"string"},
      |   {"name":"MovieName","type":"string"},
      |   {"name":"MovieReleaseDate","type":"string"},
      |   {"name":"MovieLength","type":"int"},
      |   {"name":"MovieGenre","type":"string"}
      |  ]
      |}
    """.stripMargin
  lazy val schemaMultiWatchHistory =
    """
      |{
      | "type":"record",
      | "name":"ODVS_WatchHistory",
      | "fields":[
      |   {"name":"CId","type":"int"},
      |   {"name":"CustomerTimeWatched","type":"long"},
      |   {"name":"CustomerPausedTime","type":"long"},
      |   {"name":"MovieID","type":"string"},
      |   {"name":"MovieName","type":"string"}
      |  ]
      |}
    """.stripMargin
  lazy val schemaMultiCustomerRatings =
    """
      |{
      | "type":"record",
      | "name":"ODVS_CustomerRatings",
      | "fields":[
      |   {"name":"CId","type":"int"},
      |   {"name":"MovieID","type":"string"},
      |   {"name":"MovieName","type":"string"},
      |   {"name":"CName","type":"string"},
      |   {"name":"MovieRating","type":"string"}
      |  ]
      |}
    """.stripMargin
  lazy val schemaMultiCustomerQueue =
    """
      |{
      | "type":"record",
      | "name":"ODVS_CustomerQueue",
      | "fields":[
      |   {"name":"CId","type":"int"},
      |   {"name":"CustomerTimeWatched","type":"long"},
      |   {"name":"CName","type":"string"},
      |   {"name":"MovieID","type":"string"},
      |   {"name":"MovieName","type":"string"}
      |  ]
      |}
    """.stripMargin
  lazy val schemaMultiMovieGenre =
    """
      |{
      | "type":"record",
      | "name":"ODVS_MovieGenre",
      | "fields":[
      |   {"name":"MovieGenre","type":"string"},
      |   {"name":"MovieReleaseDate","type":"string"},
      |   {"name":"MovieID","type":"string"},
      |   {"name":"MovieLength","type":"int"},
      |   {"name":"MovieName","type":"string"}
      |  ]
      |}
    """.stripMargin

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
    val schema = new Schema.Parser().parse(schemaLine)
    val datum: GenericRecord = new GenericData.Record(schema)
    datum.put("CId", odvsEvent.cId)
    datum.put("CName", odvsEvent.cName)
    datum.put("UserActive", odvsEvent.userActive)
    datum.put("TimeStamp", odvsEvent.cWatchInit)
    datum.put("PauseTime", odvsEvent.cWatchPauseTime)
    datum.put("MovieRating", odvsEvent.cMovieRating)
    datum.put("MovieID", odvsEvent.mId)
    datum.put("MovieName", odvsEvent.mName)
    datum.put("MovieReleaseDate", odvsEvent.mReleaseDate)
    datum.put("MovieLength", odvsEvent.mLength)
    datum.put("MovieGenre", odvsEvent.mGenre)
    datum
  }

  def avroMultiEvent(odvsEvent: ODVSEvent) = {
    val schemaWatchHistory = new Schema.Parser().parse(schemaMultiWatchHistory)
    val datumWatchHistory: GenericRecord = new GenericData.Record(schemaWatchHistory)
    datumWatchHistory.put("CId", odvsEvent.cId)
    datumWatchHistory.put("CustomerTimeWatched", odvsEvent.cWatchInit)
    datumWatchHistory.put("CustomerPausedTime", odvsEvent.cWatchPauseTime)
    datumWatchHistory.put("MovieID", odvsEvent.mId)
    datumWatchHistory.put("MovieName", odvsEvent.mName)

    val schemaCustomerRatings = new Schema.Parser().parse(schemaMultiCustomerRatings)
    val datumCustomerRatings: GenericRecord = new GenericData.Record(schemaCustomerRatings)
    datumCustomerRatings.put("CId", odvsEvent.cId)
    datumCustomerRatings.put("MovieID", odvsEvent.mId)
    datumCustomerRatings.put("MovieName", odvsEvent.mName)
    datumCustomerRatings.put("CName", odvsEvent.cName)
    datumCustomerRatings.put("MovieRating", odvsEvent.cMovieRating)

    val schemaCustomerQueue = new Schema.Parser().parse(schemaMultiCustomerQueue)
    val datumCustomerQueue: GenericRecord = new GenericData.Record(schemaCustomerQueue)
    datumCustomerQueue.put("CId", odvsEvent.cId)
    datumCustomerQueue.put("CustomerTimeWatched", odvsEvent.cWatchInit)
    datumCustomerQueue.put("CName", odvsEvent.cName)
    datumCustomerQueue.put("MovieID", odvsEvent.mId)
    datumCustomerQueue.put("MovieName", odvsEvent.mName)

    val schemaMovieGenre = new Schema.Parser().parse(schemaMultiMovieGenre)
    val datumMovieGenre: GenericRecord = new GenericData.Record(schemaMovieGenre)
    datumMovieGenre.put("MovieGenre", odvsEvent.mGenre)
    datumMovieGenre.put("MovieReleaseDate", odvsEvent.mReleaseDate)
    datumMovieGenre.put("MovieID", odvsEvent.mId)
    datumMovieGenre.put("MovieLength", odvsEvent.mLength)
    datumMovieGenre.put("MovieName", odvsEvent.mName)

    Map[String, GenericRecord](
      "WatchHistory" -> datumWatchHistory,
      "CustomerRatings" -> datumCustomerRatings,
      "CustomerQueue" -> datumCustomerQueue,
      "MovieGenre" -> datumMovieGenre
    )
  }

  def run() = {
    val totalEvents = eventsEndRange - eventsStartRange + 1
    var batchCount: Int = 0
    var outputFileHandler: FileHandler = null
    var outputAvroFileHandler: AvroFileHandler = null
    var outputFileWatchHistoryHandler: FileHandler = null
    var outputFileCustomerRatingsHandler: FileHandler = null
    var outputFileCustomerQueueHandler: FileHandler = null
    var outputFileMovieGenreHandler: FileHandler = null
    var outputAvroFileWatchHistoryHandler: AvroFileHandler = null
    var outputAvroFileCustomerRatingsHandler: AvroFileHandler = null
    var outputAvroFileCustomerQueueHandler: AvroFileHandler = null
    var outputAvroFileMovieGenreHandler: AvroFileHandler = null
    var eventsText: ArrayBuffer[String] = null
    var watchHistoryEventsText: ArrayBuffer[String] = null
    var customerRatingsEventsText: ArrayBuffer[String] = null
    var customerQueueEventsText: ArrayBuffer[String] = null
    var movieGenreEventsText: ArrayBuffer[String] = null
    var eventsAvro: ArrayBuffer[GenericRecord] = null
    var watchHistoryEventsAvro: ArrayBuffer[GenericRecord] = null
    var customerRatingsEventsAvro: ArrayBuffer[GenericRecord] = null
    var customerQueueEventsAvro: ArrayBuffer[GenericRecord] = null
    var movieGenreEventsAvro: ArrayBuffer[GenericRecord] = null
    var multiTableText: Map[String, String] = null
    var multiTableAvro: Map[String, GenericRecord] = null


    if (config.multiTable) {
      if (config.fileFormat == "avro") {
        outputAvroFileWatchHistoryHandler = new AvroFileHandler(new File(config.filePath, s"odvs_watch_history_$threadName.data").toString, schemaMultiWatchHistory, config.fileRollSize)
        outputAvroFileCustomerRatingsHandler = new AvroFileHandler(new File(config.filePath, s"odvs_customer_rating_$threadName.data").toString, schemaMultiCustomerRatings, config.fileRollSize)
        outputAvroFileCustomerQueueHandler = new AvroFileHandler(new File(config.filePath, s"odvs_customer_queue_$threadName.data").toString, schemaMultiCustomerQueue ,config.fileRollSize)
        outputAvroFileMovieGenreHandler = new AvroFileHandler(new File(config.filePath, s"odvs_movie_genre_$threadName.data").toString, schemaMultiMovieGenre, config.fileRollSize)
        watchHistoryEventsAvro = new ArrayBuffer[GenericRecord](config.flushBatch)
        customerRatingsEventsAvro = new ArrayBuffer[GenericRecord](config.flushBatch)
        customerQueueEventsAvro = new ArrayBuffer[GenericRecord](config.flushBatch)
        movieGenreEventsAvro = new ArrayBuffer[GenericRecord](config.flushBatch)
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
      if (config.fileFormat == "avro") {
        outputAvroFileHandler = new AvroFileHandler(new File(config.filePath,s"odvs_$threadName.data").toString, schemaLine, config.fileRollSize)
        eventsAvro = new ArrayBuffer[GenericRecord](config.flushBatch)
      } else {
        outputFileHandler = new FileHandler(new File(config.filePath, s"odvs_$threadName.data").toString, config.fileRollSize)
        eventsText  = new ArrayBuffer[String](config.flushBatch)
      }
    }

    var odvsEvent: ODVSEvent = null

    try {
      if (config.fileFormat == "avro") {
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

      var textPlaceHolder:String = null
      var avroPlaceHolder:GenericRecord = null
      (eventsStartRange to eventsEndRange).foreach { eventCount =>
        batchCount += 1
        odvsEvent = new ODVSGenerator(customersMap, movie).eventGenerate

        /*
         * Fill the buffers
         */
        if (config.multiTable) {
          if (config.fileFormat == "avro") {
            multiTableAvro = avroMultiEvent(odvsEvent)
            watchHistoryEventsAvro += multiTableAvro("WatchHistory")
            sizeCounter.getAndAdd(multiTableAvro("WatchHistory").toString.getBytes.length)
            customerRatingsEventsAvro += multiTableAvro("CustomerRatings")
            sizeCounter.getAndAdd(multiTableAvro("CustomerRatings").toString.getBytes.length)
            customerQueueEventsAvro += multiTableAvro("CustomerQueue")
            sizeCounter.getAndAdd(multiTableAvro("CustomerQueue").toString.getBytes.length)
            movieGenreEventsAvro += multiTableAvro("MovieGenre")
            sizeCounter.getAndAdd(multiTableAvro("MovieGenre").toString.getBytes.length)

          } else {
            multiTableText = formatEventMultiToString(odvsEvent, config.fileFormat)
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
          if (config.fileFormat == "avro") {
            avroPlaceHolder = avroEvent(odvsEvent)
            eventsAvro += avroPlaceHolder
            sizeCounter.getAndAdd(avroPlaceHolder.toString.getBytes.length)
          } else {
            textPlaceHolder = formatEventToString(odvsEvent, config.fileFormat)
            eventsText += textPlaceHolder
            sizeCounter.getAndAdd(textPlaceHolder.getBytes.length)
          }
        }
        // increment universal record counter
        counter.getAndIncrement
        if (batchCount == config.flushBatch || batchCount == totalEvents) {
          if (config.multiTable) {
            if (config.fileFormat == "avro") {
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
            if (config.fileFormat == "avro") {
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
        if (config.fileFormat == "avro") {
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
        if (config.fileFormat == "avro") {
          outputAvroFileHandler.close()
        } else {
          outputFileHandler.close()
        }
      }
    }
  }
}
