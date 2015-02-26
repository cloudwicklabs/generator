package com.cloudwick.generator.logEvents

/**
 * Case class for wrapping generator.log event
 * @author ashrith
 */
case class LogEvent(ip:String, timestamp:String, request:String, responseCode:Int, responseSize:Int, userAgent:String) {
  override def toString = s"$ip - - [$timestamp]" + " \"GET " + request + " HTTP/1.1\"" +
      s" $responseCode $responseSize " + "\"-\" \"" + userAgent + "\"\n"
}