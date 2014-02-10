package com.cloudwick.generator.logEvents

/**
 * Case class for wrapping generator.log event
 * @author ashrith
 */
case class LogEvent(ip:String, timestamp:String, request:String, responseCode:Int, responseSize:Int, userAgent:String)