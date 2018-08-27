package com.lotr_weather

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

trait LotrSparkInit {
  private val master = "local[1]"
  private val appName = "LOTR Weather"

  val spark: SparkSession = SparkSession.builder().appName(appName).master(master).getOrCreate()
  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  final val RESOURCE_ROOT = "src/main/resources/"
  final val DATE_FORMAT: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd")
  final val TA_DATE_FORMAT: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  //File stores weather station details from across the world. It
  final val STATION_FILE = "ghcnd-stations.txt"

  //File stores actual weather data for each day of year 2015 for weather stations listed in STATION_FILE
  final val RECORDS_FILE = "2015_copy.csv"

  //Year for which record file contains this data
  final val RECORD_YEAR = "2015"

  //This mapping file has names of LOTR locations mapped to their real earth locations.
  final val LOTR_LOCATION_MAP = "lotr_map.txt"

  //Monthly increase increase in temperature because of middle earth warming
  final val MONTHLY_INC = 1.2

  //Scientific variables to calculate atmospheric pressure based on temperature and elevation
  final val TEMPERATURE_LAPSE = 0.0065
  final val STANDARD_PRESSURE = 101325.0
  final val KELVIN = 273.15
  final val STANDARD_TEMPERATURE: Double = 15 + KELVIN
  final val GAS_CONSTANT = 8.31447
  final val MOLAR = 0.0289644
  final val GRAVITY = 9.80665
}


