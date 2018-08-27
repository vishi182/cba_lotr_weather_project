package com.lotr_weather

import com.lotr_weather.GenerateElements.{LIST_CITIES, LOTR_PLACES, sc}
import org.joda.time.Days

import scala.util.Random


object WeatherGenerator extends LotrSparkInit {

  private final val TA_FROM_DATE = "30180923"
  private final val TA_TO_DATE = "30180923"


  def setRandomDate() : String = {
    val fromDate = DATE_FORMAT.parseDateTime(TA_FROM_DATE)
    val toDate = DATE_FORMAT.parseDateTime(TA_TO_DATE)
    val diff = Days.daysBetween(fromDate, toDate).getDays
    val rand = new Random()
    val date = fromDate.plusDays(rand.nextInt(diff)).plusHours(rand.nextInt(12)).toDateTimeISO
    date.toString
  }

  def run(): Unit ={
    sc.broadcast(LOTR_PLACES)

    val date = setRandomDate()
    GenerateElements.processData(date, LIST_CITIES)
    spark.stop()
  }

  def runWithDate(date: String) : Unit ={
    sc.broadcast(LOTR_PLACES)
    GenerateElements.processData(date, LIST_CITIES)
    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      run()
    }
    else if (args.length == 1) {
      try {
        runWithDate(args(0))
      } catch {
        case ex : IllegalArgumentException =>
          println(ex.getMessage)

      }
    }
  }

}
