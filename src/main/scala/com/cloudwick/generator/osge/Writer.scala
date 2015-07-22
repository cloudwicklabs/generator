package com.cloudwick.generator.osge

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.atomic.AtomicLong

import com.cloudwick.generator.avro.{Customer, Fact, OSGERecord, Revenue}
import com.cloudwick.generator.utils._
import org.apache.avro.specific.SpecificRecord
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Writes events to file
 * @author ashrith
 */
class Writer(eventsStartRange: Int,
             eventsEndRange: Int,
             counter: AtomicLong,
             sizeCounter: AtomicLong,
             config: OptionsConfig) extends Runnable with LazyLogging {
  lazy val utils = new Utils
  lazy val dateUtils = new DateUtils
  lazy val dateFormatter = new SimpleDateFormat("dd-MMM-yy HH:mm:ss")

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
        dateUtils.genDate(dateFormatter.format(osgeEvent.cRegisterDate), dateFormatter.format(Calendar.getInstance().getTimeInMillis)))
    }
    ms
  }

  def avroEvent(event: OSGEEvent) = {
    OSGERecord.newBuilder()
      .setCId(event.cID)
      .setCName(event.cName)
      .setCEmail(event.cEmail)
      .setCGender(event.cGender)
      .setCAge(event.cAge)
      .setCAddress(event.cAddress)
      .setCCountry(event.cCountry)
      .setCRegisterDate(event.cRegisterDate)
      .setCFriendCount(event.cFriendCount)
      .setCLifeTime(event.cLifeTime)
      .setCityPlayedCount(event.cityGamePlayed)
      .setPictionaryPlayedCount(event.pictionaryGamePlayed)
      .setScramblePlayedCount(event.scrambleGamePlayed)
      .setSniperPlayedCount(event.sniperGamePlayed)
      .setCRevenue(event.cRevenue)
      .setPaidSubscriber(event.paidSubscriber)
      .build()
  }

  def customerEvent(event: OSGEEvent) = {
    Customer.newBuilder()
      .setCId(event.cID)
      .setCName(event.cName)
      .setCGender(event.cGender)
      .setCAge(event.cAge)
      .setCRegisterDate(event.cRegisterDate)
      .setCCountry(event.cCountry)
      .setCFriendCount(event.cFriendCount)
      .setCLifeTime(event.cLifeTime)
      .build()
  }

  def revenueEvent(event: OSGEEvent) = {
    Revenue.newBuilder()
      .setCId(event.cID)
      .setCPaidDate(event.paidDate)
      .setCRevenue(event.cRevenue)
      .build()
  }

  def factEvent(event: OSGEEvent) = {
    val gamesProbMap =  if (event.cGender == "female") {
      Customers.GAMES_FEMALE_PROBABILITY
    } else {
      Customers.GAMES_MALE_PROBABILITY
    }

    Fact.newBuilder()
      .setCId(event.cID)
      .setCGamePlayed(utils.pickWeightedKey(gamesProbMap))
      .setCGamePlayedDate(
        dateUtils.genDate(dateFormatter.format(event.cRegisterDate),
        dateFormatter.format(Calendar.getInstance().getTimeInMillis))
      )
      .build()
  }

  def run() = {
    val totalEvents = eventsEndRange - eventsStartRange + 1
    var batchCount: Int = 0
    var outputFileHandler: FileHandler = null
    var outputAvroFileHandler: AvroFileHandler[OSGERecord] = null
    var outputFileCustomerHandler: FileHandler = null
    var outputFileRevenueHandler: FileHandler = null
    var outputFileFactHandler: FileHandler = null
    var outputAvroFileCustomerHandler: AvroFileHandler[Customer] = null
    var outputAvroFileRevenueHandler: AvroFileHandler[Revenue] = null
    var outputAvroFileFactHandler: AvroFileHandler[Fact] = null
    var eventsText: ArrayBuffer[String] = null
    var customerEventsText: ArrayBuffer[String] = null
    var revenueEventsText: ArrayBuffer[String] = null
    var factEventsText: ArrayBuffer[String] = null
    var eventsAvro: ArrayBuffer[OSGERecord] = null
    var customerEventsAvro: ArrayBuffer[Customer] = null
    var revenueEventsAvro: ArrayBuffer[Revenue] = null
    var factEventsAvro: ArrayBuffer[Fact] = null
    var multiTableText: mutable.Map[String, ArrayBuffer[String]] = null
    var multiTableAvro: mutable.Map[String, ArrayBuffer[SpecificRecord]] = null
    var customer: Customer = null
    var fact: Fact = null
    var revenue: Revenue = null

    if (config.multiTable) {
      if (config.outputFormat == "avro") {
        outputAvroFileCustomerHandler = new AvroFileHandler[Customer](new File(config.filePath, s"osge_customers_$threadName.data").toString, config.fileRollSize)
        outputAvroFileRevenueHandler = new AvroFileHandler[Revenue](new File(config.filePath, s"osge_revenue_$threadName.data").toString, config.fileRollSize)
        outputAvroFileFactHandler = new AvroFileHandler[Fact](new File(config.filePath, s"osge_fact_$threadName.data").toString, config.fileRollSize)
        customerEventsAvro = new ArrayBuffer[Customer](config.flushBatch)
        revenueEventsAvro = new ArrayBuffer[Revenue](config.flushBatch)
        factEventsAvro = new ArrayBuffer[Fact](config.flushBatch)
      } else {
        outputFileCustomerHandler = new FileHandler(new File(config.filePath, s"osge_customers_$threadName.data").toString, config.fileRollSize)
        outputFileRevenueHandler = new FileHandler(new File(config.filePath, s"osge_revenue_$threadName.data").toString, config.fileRollSize)
        outputFileFactHandler = new FileHandler(new File(config.filePath, s"osge_fact_$threadName.data").toString, config.fileRollSize)
        customerEventsText  = new ArrayBuffer[String](config.flushBatch)
        revenueEventsText  = new ArrayBuffer[String](config.flushBatch)
        factEventsText  = new ArrayBuffer[String](config.flushBatch)
      }
    } else {
      if (config.outputFormat == "avro") {
        outputAvroFileHandler = new AvroFileHandler[OSGERecord](new File(config.filePath,s"osge_$threadName.data").toString, config.fileRollSize)
        eventsAvro = new ArrayBuffer[OSGERecord](config.flushBatch)
      } else {
        outputFileHandler = new FileHandler(new File(config.filePath, s"osge_$threadName.data").toString, config.fileRollSize)
        eventsText  = new ArrayBuffer[String](config.flushBatch)
      }
    }

    var osgeEvent: OSGEEvent = null

    try {
      if (config.outputFormat == "avro") {
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
      var avroPlaceHolder:OSGERecord = null
      (eventsStartRange to eventsEndRange).foreach { eventCount =>
        batchCount += 1
        osgeEvent = new OSGEGenerator().eventGenerate

        /*
         * Fill the buffers
         */
        if (config.multiTable) {
          if (config.outputFormat == "avro") {
            customer = customerEvent(osgeEvent)
            customerEventsAvro += customer
            sizeCounter.getAndAdd(sizeCounter.getAndAdd(customer.toString.getBytes.length))
            if (osgeEvent.cRevenue != 0) {
              revenue = revenueEvent(osgeEvent)
              revenueEventsAvro += revenue
              sizeCounter.getAndAdd(sizeCounter.getAndAdd(revenue.toString.getBytes.length))
            }
            1 to osgeEvent.cLifeTime foreach { _ =>
              fact = factEvent(osgeEvent)
              factEventsAvro += fact
              sizeCounter.getAndAdd(sizeCounter.getAndAdd(fact.toString.getBytes.length))
            }
          } else {
            multiTableText = formatEventMultiToString(osgeEvent, config.outputFormat)
            customerEventsText ++= multiTableText("Customer")
            multiTableText("Customer").map(x => sizeCounter.getAndAdd(x.getBytes.length))
            revenueEventsText ++= multiTableText("Revenue")
            multiTableText("Revenue").map(x => sizeCounter.getAndAdd(x.getBytes.length))
            factEventsText ++= multiTableText("Fact")
            multiTableText("Fact").map(x => sizeCounter.getAndAdd(x.getBytes.length))
          }
        } else {
          if (config.outputFormat == "avro") {
            avroPlaceHolder = avroEvent(osgeEvent)
            eventsAvro += avroPlaceHolder
            sizeCounter.getAndAdd(avroPlaceHolder.toString.getBytes.length)
          } else {
            textPlaceHolder = formatEventToString(osgeEvent, config.outputFormat)
            eventsText += textPlaceHolder
            sizeCounter.getAndAdd(textPlaceHolder.getBytes.length)
          }
        }
        // increment universal record counter
        counter.getAndIncrement
        if (batchCount == config.flushBatch || batchCount == totalEvents) {
          if (config.multiTable) {
            if (config.outputFormat == "avro") {
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
          outputAvroFileCustomerHandler.close()
          outputAvroFileRevenueHandler.close()
          outputAvroFileFactHandler.close()
        } else {
          outputFileCustomerHandler.close()
          outputFileRevenueHandler.close()
          outputFileFactHandler.close()
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