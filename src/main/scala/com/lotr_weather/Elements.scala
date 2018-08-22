package com.lotr_weather

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

case class ThirdAgeDate (date: String)  {
  val format: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd")
  val ta_date: DateTime = format.parseDateTime(date)

  def toTADate: String = "T.A." + ta_date.plusHours(12).toString
}

case class Condition (value: String) extends AnyVal {}

case class Climate (value: String) extends AnyVal {}

case class Temperature(value: Double, unit: String = "Celsius") {}

case class Pressure(value: Double, unit: String = "hPa") {}

case class Humidity(value: Double, unit: String = "Percent") {}

case class Location (latitude: Double, longitude: Double, elevation : Double) {
  def getElevation(elevation: String) : Double = this.elevation
}


