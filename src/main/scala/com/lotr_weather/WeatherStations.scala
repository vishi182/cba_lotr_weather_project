package com.lotr_weather

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame


object WeatherStations extends LotrSparkInit {

  /**
    *
    * @param stn_id Station ID
    * @param name Earth location name
    */

  case class Stations (stn_id: String,
                       name: String){}

  /**
    *
    * @param sparkContext The spark context variable used to read input file
    * @param fileName     Input file mapping real earth location names against their respective weather station ID.
    *                     Method reads a fixed lengths character file and retrieves output of case class Stations
    *                     * ------------------------------
    *                     * Variable   Columns   Type
    *                     * ------------------------------
    *                     * ID            1-11   Character
    *                     * NAME         42-71   Character
    *                     * ------------------------------
    * @return DataFrame of station id and earth location name
    */
  def loadStationsData(sparkContext: SparkContext, fileName: String): DataFrame = {
    import spark.implicits._
    val rows = sparkContext.
      textFile(RESOURCE_ROOT + fileName).
      map(r => (r.substring(0,11).trim, r.substring(41,71).trim())).
      map({ case (e1, e2) => Stations(e1.toString, e2.toString)}).
      toDF()
    rows
  }

}

