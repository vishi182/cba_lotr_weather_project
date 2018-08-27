package com.lotr_weather

import com.lotr_weather.GenerateElements.{LIST_CITIES, LOTR_PLACES, sc}
import org.joda.time.{Days, Months}

import scala.util.Random


object WeatherGenerator extends LotrSparkInit {

  private final val TA_FROM_DATE = "30180923"
  private final val TA_TO_DATE = "30210929"


  def setRandomDate() : String = {
    val fromDate = DATE_FORMAT.parseDateTime(TA_FROM_DATE)
    val toDate = DATE_FORMAT.parseDateTime(TA_TO_DATE)
    val diff = Days.daysBetween(fromDate, toDate).getDays
    val rand = new Random()
    val date = fromDate.plusDays(rand.nextInt(diff)).plusHours(rand.nextInt(12)).toDateTimeISO
    date.toString
  }

  /** This method is used to calculate months from when Sauron's activities started impacting the Middle Earth.
    * These months will then be multiplied by Temperature to get warming impact
    *
    * @param date input date String
    * @return
    *         0 - Before T.A. 23 Sep 3018. No impact was found on middle earth. That's before Frodo's journey started.
    *         1 to 36 - During War of the Rings. Sauron's activity started impacting the atmosphere of middle earth.
    *         36 - Middle Earth warming will hit plateau after T.A. 29 Sep 3021. The impact will stay like this.
    *              No data post this date is available. Weather report will be based on last noticed effects.
    */
  def getImpactedMonths(date: String): Int = {
    val parse_date = DATE_FORMAT.parseDateTime(date).toDateTime
    val fromDate = DATE_FORMAT.parseDateTime(TA_FROM_DATE).toDateTime
    val toDate = DATE_FORMAT.parseDateTime(TA_TO_DATE).toDateTime
    val ret = if (parse_date.isBefore(fromDate)) {
      val before_months = Months.monthsBetween(parse_date, fromDate).getMonths
      val ret = 0
      println(s"War of the Rings has not yet started.$before_months months left before Frodo starts his journey and Sauron prepares for middle earth's conquer.")
      ret
    }
    else if (parse_date.isAfter(fromDate) && parse_date.isBefore(toDate)) {
      val ret: Int = Months.monthsBetween(parse_date, toDate).getMonths
      println(s"War of the Rings has begun. $ret months before Sauron becomes Lord of the Rings and conquers the Middle Earth, Sauron's war preparation has started to warm up the middle earth.")
      ret
    }
    else if (parse_date.isAfter(toDate)) {
      val end_month: Int = Months.monthsBetween(toDate, parse_date).getMonths
      println(s"It has been $end_month since War of the Rings ended. Sauron has the Ring and his domination of Middle Earth is complete. His activities and support for Orcs have given a serious blow to the flora and fauna of the middle earth. The average temperature has risen by 5 degree Celsius.")
      /**
        * No impact will be considered after T.A. 29 Sep 3021
        */
      val month: Int = Months.monthsBetween(fromDate, toDate).getMonths
      month
    }
    ret.asInstanceOf[Int]
  }

  def run(): Unit ={
    sc.broadcast(LOTR_PLACES)

    val date = setRandomDate()
    val impact = getImpactedMonths(date)
    GenerateElements.processData(date, LIST_CITIES, impact)
    spark.stop()
  }

  def runWithDate(date: String) : Unit ={
    sc.broadcast(LOTR_PLACES)
    val impact = getImpactedMonths(date)
    GenerateElements.processData(date, LIST_CITIES, impact)
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
