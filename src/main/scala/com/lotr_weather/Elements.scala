package com.lotr_weather

import org.apache.spark.sql.DataFrame
import org.joda.time.format.DateTimeFormat

import scala.util.Random


object Elements extends LotrSparkInit{

  case class Place (value: String) extends AnyVal {}

  case class Location (latitude: Double, longitude: Double, elevation : Double) {}

  case class ThirdAgeDate (value: String) extends AnyVal {}

  case class Condition (value: String) extends AnyVal {}

  case class Temperature (value: Double, unit: String = "Celsius") {}

  case class Pressure(value: Double, unit: String = "hPa") {}

  case class Humidity(value: Double, unit: String = "Percent") {}


  def getDateString(date: String) : String = date.replace(date.substring(0,4), RECORD_YEAR)

  def toTADate(date: String): ThirdAgeDate = {
    val full_date = DATE_FORMAT.parseDateTime(date).plusHours(Random.nextInt(12)).plusMinutes(Random.nextInt(60))
    val ta_date = TA_DATE_FORMAT.print(full_date)
    ThirdAgeDate("T.A." + ta_date)
  }

  def checkAndReturnTempValues(df: DataFrame, sel_col: String, filter_col1: String, filter_val1: String, filter_col2: String, filter_val2: String) : Double ={
    val dtf = df.filter(df(filter_col1) === filter_val1).filter(df(filter_col2) === filter_val2).select(sel_col)
    val data = if (dtf.head(1).isEmpty) -999 else dtf.first().getString(0).toDouble
    data
  }

  def getTemperatureCondition(date: String, lotr_places: DataFrame, lotr_city: String) : (Temperature, Condition) = {
    val data: DataFrame = WeatherRecords.
      joinDataFrames(date, lotr_places).
      select("place", "element", "value").
      toDF("place","element", "value")
    val t_avg = checkAndReturnTempValues(data, "value", "place", lotr_city, "element", "TAVG")
    val t_max = checkAndReturnTempValues(data, "value", "place", lotr_city, "element", "TMAX")
    val t_min = checkAndReturnTempValues(data, "value", "place", lotr_city, "element", "TMIN")
    val prcp = checkAndReturnTempValues(data, "value", "place", lotr_city, "element", "PRCP")
    val snow = checkAndReturnTempValues(data, "value", "place", lotr_city, "element", "SNOW'")
    val snwd = checkAndReturnTempValues(data, "value", "place", lotr_city, "element", "SNWD")
    val temp = if (!(t_avg == -999)) t_avg else if (!(t_max == -999)) t_max else t_min
    val cond = if (snow != -999 || snow != 0.0 || snwd != -999 || snwd != 0.0) "Snow" else if (prcp != -999 || prcp != 0.0) "Rain" else "Sunny"
    (Temperature(temp), Condition(cond))
  }


  def getPressure(temp: Double, elevation: Double) : Pressure = {
    val temperatureInKelvin = KELVIN + temp
    val standardPressureWithAltitude = STANDARD_PRESSURE * Math.pow(1 - (TEMPERATURE_LAPSE * elevation / STANDARD_TEMPERATURE), (GRAVITY * MOLAR) / (GAS_CONSTANT * TEMPERATURE_LAPSE))
    val pressureWithTemperature = standardPressureWithAltitude * STANDARD_TEMPERATURE / temperatureInKelvin
    Pressure(pressureWithTemperature)
  }

  def setHumidity() : Humidity = {
    Humidity(Random.nextInt(70) + 20)
  }

  def getLocationData(place_name: String) : Location = {
    val lotr_places = LotrDetails.loadWeatherMapData(sc, LOTR_LOCATION_MAP)
    val lat = lotr_places.filter(lotr_places("place") === place_name).select("latitude").first().getDouble(0)
    val long = lotr_places.filter(lotr_places("place") === place_name).select("longitude").first().getDouble(0)
    val elev = lotr_places.filter(lotr_places("place") === place_name).select("elevation").first().getDouble(0)
    Location(lat, long, elev)
  }

}






