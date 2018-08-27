package com.lotr_weather

import org.apache.spark.sql.DataFrame
import org.joda.time.format.DateTimeFormat

import scala.util.Random


object Elements extends LotrSparkInit{

  case class Place (value: String) {}

  case class Location (latitude: Double, longitude: Double, elevation : Double, elev_unit: String = "mts") {}

  case class ThirdAgeDate (value: String) {}

  case class Condition (value: String) {}

  case class Temperature (value: Double, unit: String = "Celsius") {}

  case class Pressure(value: Double, unit: String = "hPa") {
    require(value > 0.0)
  }

  case class Humidity(value: Double, unit: String = "Percent") {}


  def getDateString(date: String) : String = date.replace(date.substring(0,4), RECORD_YEAR)

  def toTADate(date: String): ThirdAgeDate = {
    val full_date = DATE_FORMAT.parseDateTime(date).plusHours(Random.nextInt(12)).plusMinutes(Random.nextInt(60))
    val ta_date = TA_DATE_FORMAT.print(full_date)
    ThirdAgeDate("T.A." + ta_date)
  }

  /**
    *
    * @param df           Dataframe storing data from lotr_map.txt. It will be joined with Weather Records and Stations file
    * @param sel_col      select value of elements. It will give us temperature in Celsius or precipitation/snowfall in mm
    * @param filter_col1  filter on LOTR place column. It is joined with weather station id on real earth station names
    * @param filter_val1  LOTR place value
    * @param filter_col2  element column to identify weather
    * @param filter_val2
    *                       TAVG - average temp
    *                       TMAX - Max temp
    *                       TMIN - Min temp
    *                       PRCP - Precipitation in mm
    *                       SNOW - Snowfall in mm
    *                       SNWD - Snow depth in mm
    *
    * @return  value of select column sel_col for temp/prcp/snow
    */
  def checkAndReturnTempValues(df: DataFrame,
                               sel_col: String,
                               filter_col1: String,
                               filter_val1: String,
                               filter_col2: String,
                               filter_val2: String) : Double = {
    val dtf = df.
      filter(df(filter_col1) === filter_val1).
      filter(df(filter_col2) === filter_val2).
      select(sel_col)
    val data = if (dtf.head(1).isEmpty) -999 else dtf.first().getString(0).toDouble
    data
  }

  /** Selects any of avg/min/max temperature or calculates a random value if temp is not available for that date
    * It also calculates impact of Sauron's activities. It uses MONTHLY_INC to calculate increase in warming temp
    *
    * @param t_avg
    * @param t_max
    * @param t_min
    * @param impact
    * @return
    */
  def getImpactedTemperature (t_avg: Double, t_max: Double, t_min: Double, impact: Int): Double = {
    val temp = if (!(t_avg == -999)) t_avg
                else if (!(t_max == -999)) t_max
                else if (!(t_min == -999)) t_min
                else {
                    val rand = 15 + Random.nextInt(20) //Get random temp between 15 and 35
                    rand
                  }
    val impact_temp = MONTHLY_INC * temp

    //Last check to balance out extreme temperatures in Records file
    if (impact_temp > 50.0 || impact_temp < -50.0) {
      impact_temp / 10
    }
    else {
      impact_temp
    }
  }

  /** Calculates temperature and weather condition
    *
    * @param date REport date
    * @param lotr_places Dataframe of LOTR details for joining with Recordds and Station data
    * @param lotr_city LOTR City name
    * @param impact Months since Sauron's activities started impacting the weather
    * @return (Temperature, Condition) of case class types
    */
  def getTemperatureCondition(date: String, lotr_places: DataFrame, lotr_city: String, impact: Int) : (Temperature, Condition) = {
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
    val temp = getImpactedTemperature(t_avg, t_max, t_min, impact)
    val cond = if (snow > 0.0 || snwd > 0) "Snow"
                else if (prcp > 0)  "Rain"
                else "Sunny"
    (Temperature(temp), Condition(cond))
  }

  /**
    * Calculates pressure based on temperature and elevation
    *
    * @param temp
    * @param elevation
    * @return
    */
  def getPressure(temp: Double, elevation: Double) : Pressure = {
    val temperatureInKelvin = KELVIN + temp
    val standardPressureWithAltitude = STANDARD_PRESSURE * Math.pow(1 - (TEMPERATURE_LAPSE * elevation / STANDARD_TEMPERATURE), (GRAVITY * MOLAR) / (GAS_CONSTANT * TEMPERATURE_LAPSE))
    val pressureWithTemperature = standardPressureWithAltitude * STANDARD_TEMPERATURE / temperatureInKelvin
    Pressure(pressureWithTemperature)
  }

  /**
    * Sets Humidity value for any location
    *
    * @return
    */
  def setHumidity() : Humidity = {
    Humidity(Random.nextInt(70) + 20)
  }

  /**
    * Retieves Latitude, Longitude and Elevation from LOTR File for input LOTR Place
    *
    * @param place_name
    * @return
    */
  def getLocationData(place_name: String) : Location = {
    val lotr_places = LotrDetails.loadWeatherMapData(sc, LOTR_LOCATION_MAP)
    val lat = lotr_places.filter(lotr_places("place") === place_name).select("latitude").first().getDouble(0)
    val long = lotr_places.filter(lotr_places("place") === place_name).select("longitude").first().getDouble(0)
    val elev = lotr_places.filter(lotr_places("place") === place_name).select("elevation").first().getDouble(0)
    Location(lat, long, elev)
  }

}






