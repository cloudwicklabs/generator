package com.cloudwick.generator.osge

import java.util.concurrent.atomic.AtomicLong
import org.slf4j.LoggerFactory
import com.cloudwick.generator.utils.{AvroFileHandler, FileHandler, Utils}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import scala.collection.mutable.ArrayBuffer
import java.io.File
import java.util.Calendar
import java.text.SimpleDateFormat
import scala.collection.mutable

/**
 * Writes events to file
 * @author ashrith
 */
class Writer(eventsStartRange: Int,
             eventsEndRange: Int,
             counter: AtomicLong,
             sizeCounter: AtomicLong,
             config: OptionsConfig) extends Runnable {
  lazy val logger = LoggerFactory.getLogger(getClass)
  lazy val utils = new Utils
  lazy val fileUtils = new utils.FileUtils
  lazy val schemaLine =
  """
    |{
    | "type":"record",
    | "name":"OSGE",
    | "fields":[
    |   {"name":"CId","type":"string"},
    |   {"name":"CName","type":"string"},
    |   {"name":"CEmail","type":"string"},
    |   {"name":"CGender","type":"string"},
    |   {"name":"CAge","type":"int"},
    |   {"name":"CAddress","type":"string"},
    |   {"name":"CCountry","type":"string"},
    |   {"name":"CRegisterDate","type":"long"},
    |   {"name":"CFriendCount","type":"int"},
    |   {"name":"CLifeTime","type":"int"},
    |   {"name":"CityPlayedCount","type":"int"},
    |   {"name":"PictionaryPlayedCount","type":"int"},
    |   {"name":"ScramblePlayedCount","type":"int"},
    |   {"name":"SniperPlayedCount","type":"int"},
    |   {"name":"CRevenue","type":"int"},
    |   {"name":"PaidSubscriber","type":"string"}
    |  ]
    |}
  """.stripMargin
  lazy val schemaMultiCustomer =
  """
    |{
    | "type":"record",
    | "name":"OSGE_Customer",
    | "fields":[
    |   {"name":"CId","type":"string"},
    |   {"name":"CName","type":"string"},
    |   {"name":"CGender","type":"string"},
    |   {"name":"CAge","type":"int"},
    |   {"name":"CRegisterDate","type":"long"},
    |   {"name":"CCountry","type":"string"},
    |   {"name":"CFriendCount","type":"int"},
    |   {"name":"CLifeTime","type":"int"}
    |  ]
    |}
  """.stripMargin
  lazy val schemaMultiRevenue =
  """
    |{
    | "type":"record",
    | "name":"OSGE_Revenue",
    | "fields":[
    |   {"name":"CId","type":"string"},
    |   {"name":"CPaidDate","type":"long"},
    |   {"name":"CRevenue","type":"int"}
    |  ]
    |}
  """.stripMargin
  lazy val schemaMultiFact =
    """
      |{
      | "type":"record",
      | "name":"OSGE_Fact",
      | "fields":[
      |   {"name":"CId","type":"string"},
      |   {"name":"CGamePlayed","type":"string"},
      |   {"name":"CGamePlayedDate","type":"long"}
      |  ]
      |}
    """.stripMargin

  lazy val sleepTime = if(config.eventsPerSec == 0) 0 else 1000/config.eventsPerSec

  def threadName = Thread.currentThread().getName

  def formatEventToString(osgeEvent: OSGEEvent, formatter: String) = {
    val formatChar = formatter match {
      case "tsv" => '\t'
      case "csv" => ','
      case _ => '\t'
    }
    "%s%c%s%c%s%c%s%c%d%c%s%c%s%c%d%c%d%c%d%c%d%c%d%c%d%c%d%c%d%c%s\n".format(osgeEvent.cID, formatChar,
      osgeEvent.cName, formatChar, osgeEvent.cEmail, formatChar, osgeEvent.cGender, formatChar, osgeEvent.cAge,
      formatChar, osgeEvent.cAddress, formatChar, osgeEvent.cCountry, formatChar, osgeEvent.cRegisterDate, formatChar,
      osgeEvent.cFriendCount, formatChar, osgeEvent.cLifeTime, formatChar, osgeEvent.cityGamePlayed, formatChar,
      osgeEvent.pictionaryGamePlayed, formatChar, osgeEvent.scrambleGamePlayed, formatChar, osgeEvent.sniperGamePlayed,
      formatChar, osgeEvent.cRevenue, formatChar, osgeEvent.paidSubscriber)
  }

  def formatEventMultiToString(osgeEvent: OSGEEvent, formatter: String) = {
    val dateFormatter = new SimpleDateFormat("dd-MMM-yy HH:mm:ss")
    val formatChar = formatter match {
      case "tsv" => '\t'
      case "csv" => ','
      case _ => '\t'
    }

    val ms = scala.collection.mutable.Map[String, ArrayBuffer[String]](
      "Customer" -> new ArrayBuffer[String](1),
      "Revenue" -> new ArrayBuffer[String],
      "Fact" -> new ArrayBuffer[String](osgeEvent.cLifeTime)
    )

    ms("Customer") += "%s%c%s%c%s%c%d%c%d%c%s%c%d%c%d\n".format(osgeEvent.cID, formatChar, osgeEvent.cName, formatChar,
      osgeEvent.cGender, formatChar, osgeEvent.cAge, formatChar, osgeEvent.cRegisterDate, formatChar,
      osgeEvent.cCountry, formatChar, osgeEvent.cFriendCount, formatChar, osgeEvent.cLifeTime)

    if (osgeEvent.cRevenue != 0) {
      ms("Revenue") += "%s%c%d%c%d\n".format(osgeEvent.cID, formatChar, osgeEvent.paidDate, formatChar, osgeEvent.cRevenue)
    }

    1 to osgeEvent.cLifeTime foreach { _ =>
      val gamesProbMap =  if (osgeEvent.cGender == "female") {
                            Customers.GAMES_FEMALE_PROBABILITY
                          } else {
                            Customers.GAMES_MALE_PROBABILITY
                          }
      ms("Fact") += "%s%c%s%c%d\n".format(osgeEvent.cID, formatChar, utils.pickWeightedKey(gamesProbMap), formatChar,
        utils.genDate(dateFormatter.format(osgeEvent.cRegisterDate), dateFormatter.format(Calendar.getInstance().getTimeInMillis)))
    }
    ms
  }

  def avroEvent(osgeEvent: OSGEEvent) = {
    val schema = new Schema.Parser().parse(schemaLine)
    val datum: GenericRecord = new GenericData.Record(schema)
    datum.put("CId", osgeEvent.cID)
    datum.put("CName", osgeEvent.cName)
    datum.put("CEmail", osgeEvent.cEmail)
    datum.put("CGender", osgeEvent.cGender)
    datum.put("CAge", osgeEvent.cAge)
    datum.put("CAddress", osgeEvent.cAddress)
    datum.put("CCountry", osgeEvent.cCountry)
    datum.put("CRegisterDate", osgeEvent.cRegisterDate)
    datum.put("CFriendCount", osgeEvent.cFriendCount)
    datum.put("CLifeTime", osgeEvent.cLifeTime)
    datum.put("CityPlayedCount", osgeEvent.cityGamePlayed)
    datum.put("PictionaryPlayedCount", osgeEvent.pictionaryGamePlayed)
    datum.put("ScramblePlayedCount", osgeEvent.scrambleGamePlayed)
    datum.put("SniperPlayedCount", osgeEvent.sniperGamePlayed)
    datum.put("CRevenue", osgeEvent.cRevenue)
    datum.put("PaidSubscriber", osgeEvent.paidSubscriber)
    datum
  }

  def avroMultiEvent(osgeEvent: OSGEEvent) = {
    val ms = scala.collection.mutable.Map[String, ArrayBuffer[GenericRecord]](
      "Customer" -> new ArrayBuffer[GenericRecord],
      "Revenue" -> new ArrayBuffer[GenericRecord],
      "Fact" -> new ArrayBuffer[GenericRecord](osgeEvent.cLifeTime)
    )
    val dateFormatter = new SimpleDateFormat("dd-MMM-yy HH:mm:ss")

    val schemaCustomer = new Schema.Parser().parse(schemaMultiCustomer)
    val datumCustomer: GenericRecord = new GenericData.Record(schemaCustomer)
    datumCustomer.put("CId", osgeEvent.cID)
    datumCustomer.put("CName", osgeEvent.cName)
    datumCustomer.put("CGender", osgeEvent.cGender)
    datumCustomer.put("CAge", osgeEvent.cAge)
    datumCustomer.put("CRegisterDate", osgeEvent.cRegisterDate)
    datumCustomer.put("CCountry", osgeEvent.cCountry)
    datumCustomer.put("CFriendCount", osgeEvent.cFriendCount)
    datumCustomer.put("CLifeTime", osgeEvent.cLifeTime)

    ms("Customer") += datumCustomer

    if (osgeEvent.cRevenue != 0) {
      val schemaRevenue = new Schema.Parser().parse(schemaMultiRevenue)
      val datumRevenue: GenericRecord = new GenericData.Record(schemaRevenue)
      datumRevenue.put("CId", osgeEvent.cID)
      datumRevenue.put("CPaidDate", osgeEvent.paidDate)
      datumRevenue.put("CRevenue", osgeEvent.cRevenue)

      ms("Revenue") += datumRevenue
    }

    1 to osgeEvent.cLifeTime foreach { _ =>
      val gamesProbMap =  if (osgeEvent.cGender == "female") {
        Customers.GAMES_FEMALE_PROBABILITY
      } else {
        Customers.GAMES_MALE_PROBABILITY
      }
      val schemaFact = new Schema.Parser().parse(schemaMultiFact)
      val datumFact: GenericRecord = new GenericData.Record(schemaFact)
      datumFact.put("CId", osgeEvent.cID)
      datumFact.put("CGamePlayed", utils.pickWeightedKey(gamesProbMap))
      datumFact.put("CGamePlayedDate",
        utils.genDate(dateFormatter.format(osgeEvent.cRegisterDate),
          dateFormatter.format(Calendar.getInstance().getTimeInMillis)))

      ms("Fact") += datumFact
    }
    ms
  }

  def run() = {
    val totalEvents = eventsEndRange - eventsStartRange + 1
    var batchCount: Int = 0
    var outputFileHandler: FileHandler = null
    var outputAvroFileHandler: AvroFileHandler = null
    var outputFileCustomerHandler: FileHandler = null
    var outputFileRevenueHandler: FileHandler = null
    var outputFileFactHandler: FileHandler = null
    var outputAvroFileCustomerHandler: AvroFileHandler = null
    var outputAvroFileRevenueHandler: AvroFileHandler = null
    var outputAvroFileFactHandler: AvroFileHandler = null
    var eventsText: ArrayBuffer[String] = null
    var customerEventsText: ArrayBuffer[String] = null
    var revenueEventsText: ArrayBuffer[String] = null
    var factEventsText: ArrayBuffer[String] = null
    var eventsAvro: ArrayBuffer[GenericRecord] = null
    var customerEventsAvro: ArrayBuffer[GenericRecord] = null
    var revenueEventsAvro: ArrayBuffer[GenericRecord] = null
    var factEventsAvro: ArrayBuffer[GenericRecord] = null
    var multiTableText: mutable.Map[String, ArrayBuffer[String]] = null
    var multiTableAvro: mutable.Map[String, ArrayBuffer[GenericRecord]] = null


    if (config.multiTable) {
      if (config.fileFormat == "avro") {
        outputAvroFileCustomerHandler = new AvroFileHandler(new File(config.filePath, s"osge_customers_$threadName.data").toString, schemaMultiCustomer, config.fileRollSize)
        outputAvroFileRevenueHandler = new AvroFileHandler(new File(config.filePath, s"osge_revenue_$threadName.data").toString, schemaMultiRevenue, config.fileRollSize)
        outputAvroFileFactHandler = new AvroFileHandler(new File(config.filePath, s"osge_fact_$threadName.data").toString, schemaMultiFact,config.fileRollSize)
        customerEventsAvro = new ArrayBuffer[GenericRecord](config.flushBatch)
        revenueEventsAvro = new ArrayBuffer[GenericRecord](config.flushBatch)
        factEventsAvro = new ArrayBuffer[GenericRecord](config.flushBatch)
      } else {
        outputFileCustomerHandler = new FileHandler(new File(config.filePath, s"osge_customers_$threadName.data").toString, config.fileRollSize)
        outputFileRevenueHandler = new FileHandler(new File(config.filePath, s"osge_revenue_$threadName.data").toString, config.fileRollSize)
        outputFileFactHandler = new FileHandler(new File(config.filePath, s"osge_fact_$threadName.data").toString, config.fileRollSize)
        customerEventsText  = new ArrayBuffer[String](config.flushBatch)
        revenueEventsText  = new ArrayBuffer[String](config.flushBatch)
        factEventsText  = new ArrayBuffer[String](config.flushBatch)
      }
    } else {
      if (config.fileFormat == "avro") {
        outputAvroFileHandler = new AvroFileHandler(new File(config.filePath,s"osge_$threadName.data").toString, schemaLine, config.fileRollSize)
        eventsAvro = new ArrayBuffer[GenericRecord](config.flushBatch)
      } else {
        outputFileHandler = new FileHandler(new File(config.filePath, s"osge_$threadName.data").toString, config.fileRollSize)
        eventsText  = new ArrayBuffer[String](config.flushBatch)
      }
    }

    var osgeEvent: OSGEEvent = null

    try {
      if (config.fileFormat == "avro") {
        if (config.multiTable) {
          outputAvroFileCustomerHandler.openFile()
          outputAvroFileRevenueHandler.openFile()
          outputAvroFileFactHandler.openFile()
        } else {
          outputAvroFileHandler.openFile()
        }
      } else {
        if (config.multiTable) {
          outputFileCustomerHandler.openFile()
          outputFileRevenueHandler.openFile()
          outputFileFactHandler.openFile()
        } else {
          outputFileHandler.openFile()
        }
      }

      var textPlaceHolder:String = null
      var avroPlaceHolder:GenericRecord = null
      (eventsStartRange to eventsEndRange).foreach { eventCount =>
        batchCount += 1
        osgeEvent = new OSGEGenerator().eventGenerate

        /*
         * Fill the buffers
         */
        if (config.multiTable) {
          if (config.fileFormat == "avro") {
            multiTableAvro = avroMultiEvent(osgeEvent)
            customerEventsAvro ++= multiTableAvro("Customer")
            multiTableAvro("Customer").map(x => sizeCounter.getAndAdd(x.toString.getBytes.length))
            revenueEventsAvro ++= multiTableAvro("Revenue")
            multiTableAvro("Revenue").map(x => sizeCounter.getAndAdd(x.toString.getBytes.length))
            factEventsAvro ++= multiTableAvro("Fact")
            multiTableAvro("Fact").map(x => sizeCounter.getAndAdd(x.toString.getBytes.length))
          } else {
            multiTableText = formatEventMultiToString(osgeEvent, config.fileFormat)
            customerEventsText ++= multiTableText("Customer")
            multiTableText("Customer").map(x => sizeCounter.getAndAdd(x.getBytes.length))
            revenueEventsText ++= multiTableText("Revenue")
            multiTableText("Revenue").map(x => sizeCounter.getAndAdd(x.getBytes.length))
            factEventsText ++= multiTableText("Fact")
            multiTableText("Fact").map(x => sizeCounter.getAndAdd(x.getBytes.length))
          }
        } else {
          if (config.fileFormat == "avro") {
            avroPlaceHolder = avroEvent(osgeEvent)
            eventsAvro += avroPlaceHolder
            sizeCounter.getAndAdd(avroPlaceHolder.toString.getBytes.length)
          } else {
            textPlaceHolder = formatEventToString(osgeEvent, config.fileFormat)
            eventsText += textPlaceHolder
            sizeCounter.getAndAdd(textPlaceHolder.getBytes.length)
          }
        }
        // increment universal record counter
        counter.getAndIncrement
        if (batchCount == config.flushBatch || batchCount == totalEvents) {
          if (config.multiTable) {
            if (config.fileFormat == "avro") {
              outputAvroFileCustomerHandler.publishBuffered(customerEventsAvro)
              outputAvroFileRevenueHandler.publishBuffered(revenueEventsAvro)
              outputAvroFileFactHandler.publishBuffered(factEventsAvro)
              customerEventsAvro.clear()
              revenueEventsAvro.clear()
              factEventsAvro.clear()
            } else {
              outputFileCustomerHandler.publishBuffered(customerEventsText)
              outputFileRevenueHandler.publishBuffered(revenueEventsText)
              outputFileFactHandler.publishBuffered(factEventsText)
              customerEventsText.clear()
              revenueEventsText.clear()
              factEventsText.clear()
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
          outputAvroFileCustomerHandler.close()
          outputAvroFileRevenueHandler.close()
          outputAvroFileFactHandler.close()
        } else {
          outputFileCustomerHandler.close()
          outputFileRevenueHandler.close()
          outputFileFactHandler.close()
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