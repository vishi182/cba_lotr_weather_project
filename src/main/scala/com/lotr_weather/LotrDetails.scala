package com.lotr_weather

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset}



object LotrDetails extends LotrSparkInit {

  /**
    *
    * @param name Real earth city name
    * @param place LOTR location name
    * @param latitude LOTR location Latitude
    * @param longitude LOTR location Fictional longitude
    * @param elevation LOTR location Fictional elevation
    */
  case class WeatherMap (name: String,
                         place: String,
                         latitude: Double,
                         longitude: Double,
                         elevation: Double) {}


  /**
    *
    * @param sparkContext The spark context variable used to read input file
    * @param fileName Input file mapping real earth locations to their respective LOTR locations, fictional coordinates and elevation levels
    * @return Dataset of type WeatherMap
    *
    */

  def loadWeatherMapData(sparkContext: SparkContext, fileName: String): DataFrame = {//Dataset[WeatherMap] = {
  val read_map = sparkContext.
    textFile(RESOURCE_ROOT + fileName).
    filter(!_.contains("elevation")).
    map(_.split(",")).
    map(x => WeatherMap(x(0), x(1), x(2).trim.toDouble, x(3).trim.toDouble, x(4).trim.toDouble))
    import spark.implicits._
    read_map.toDF
  }

}

