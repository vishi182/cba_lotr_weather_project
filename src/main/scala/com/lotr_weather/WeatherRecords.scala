package com.lotr_weather

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame


object WeatherRecords extends LotrSparkInit {

  /**
    *
    * @param stn_id Weather Station ID
    * @param rec_date Record date
    * @param element
    *                TAVG avg temperature
    *                TMAX max temperature
    *                TMIN min temperature
    *                PRCP Precipitation
    *                SNOW snowy condition
    *                SNDW snow depth
    *
    * @param value
    *              if element is TMAX/TMIN, then temperature in degrees Celcius
    *              if element is SNOW, then snowfall in mm
    *              if element is SNDW, then snow depth in mm
    *              if element is PRCP, then precipitation in mm
    */

  case class Records (stn_id: String,
                      rec_date: String,
                      element: String,
                      value: String) {}


  /** Method loads the weather data from file into a dataframe.
    * This data will be used to get temperature, and generate weather condition of LOTR location.
    *
    * @param sparkContext The spark context variable used to read input file
    * @param fileName File containing 1 year's weather data of each station across earth
    * @return DataFrame of case class Records
    */

  def loadRecords(sparkContext: SparkContext, fileName: String): DataFrame = {
    import spark.implicits._
    val records = sparkContext.
      textFile( RESOURCE_ROOT + fileName).
      map(_.split(",")).
      map(f => Records(f(0), f(1), f(2), f(3))).
      toDF()
    records
  }

  /** Method joins LOTR map file with weather stations and weather records to generate dataframe of complete dataset for a given date
    *
    * @param date processing date
    * @param lotr_places LOTR data file loaded into dataset of case class WeatherMap
    * @return
    */
  def joinDataFrames(date: String, lotr_places: DataFrame) : DataFrame = {
    val map_date = Elements.getDateString(date)
    val stn = WeatherStations.loadStationsData(sc, STATION_FILE)
    val records = loadRecords(sc, RECORDS_FILE)
    val weather_data = stn.join(lotr_places, "name")
    val lotr_to_rec = weather_data.join(records, "stn_id")
    val complete_data = lotr_to_rec.filter(lotr_to_rec("rec_date") === date)
    complete_data
  }

}


