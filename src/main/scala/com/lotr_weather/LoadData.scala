package com.lotr_weather


import org.apache.spark.sql.{DataFrame, SparkSession}

trait SparkBase {
  private val master = "local[*]"
  private val appName = "LOTR Weather"

  val session = SparkSession.builder().appName(appName).master(master).getOrCreate()
}

class LoadData extends SparkBase {

  val RESOURCE_ROOT = "src/resources/"

  /** ------------------------------
    * Variable   Columns   Type
    * ------------------------------
    * ID            1-11   Character
    * NAME         42-71   Character
    * ------------------------------
  **/
  def loadStationsData(session: SparkSession, fileName: String): DataFrame = {
    val rows = session.read.textFile(RESOURCE_ROOT + fileName).filter(!_.isEmpty)
    val stn_data = rows
      .map(r => (
        r.substring(0,11).trim,
        r.substring(41,71).trim)
      ).map({case (f1,f2) => Stations(f1.toString, f2.toString)}).
      toDF
    stn_data
  }

  def loadWeatherMapData(session: SparkSession, fileName: String): DataFrame = {
    val weather_map = session.read.
      option("delimiter", "|").
      option("header", true).
      option("inferSchema", value = true).
      textFile(RESOURCE_ROOT + fileName).filter(!_.isEmpty).
      toDF()
    weather_map
  }

  def loadRecords(session: SparkSession, fileName: String): DataFrame = {
    val records = session.read.textFile( RESOURCE_ROOT + fileName ).filter( !_.isEmpty )
    val data = records.map(row => row.split(",")).map(f => Records(f(0), f(1), f(2), f(3), f(4),f(5), f(6))).toDF()
    data
  }

  def joinDataFrames() : Unit = {
    val stn = loadStationsData(session, "ghcnd-station.txt")
    val w_map = loadWeatherMapData(session, "weather_map.txt")
    val stn_city_map = stn.join(w_map, "stn_id")
    stn_city_map.queryExecution

  }

}
