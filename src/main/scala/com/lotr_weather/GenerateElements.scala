package com.lotr_weather

import com.lotr_weather.Elements._
import org.apache.spark.sql.DataFrame

object GenerateElements extends LotrSparkInit {

  /**
    *
    * @param place LOTR place name
    * @param location latitude, longitude and elevation values
    * @param date Third Age date
    * @param condition Weather condition Sunny/Rain/Snow
    * @param temperature in degree Celcius value
    * @param pressure atmospheric pressure in hPa
    * @param humidity air humidity in percent
    */
  case class FinalRec(place: String,
                      location: (String, String, String),
                      date: String,
                      condition: String,
                      temperature: String,
                      pressure: String,
                      humidity: String
                     ) {}

  final val LOTR_PLACES = LotrDetails.loadWeatherMapData(sc, LOTR_LOCATION_MAP)
  final val LIST_CITIES: List[String] = LOTR_PLACES.select("place").rdd.map(f => f(0).toString).collect().toList

  /** This method will process all the files and generate report. The report will be saved in file - "out/LOTRWeather.txt"
    *
    * @param date Date of data processing
    * @param lotr_list List of cities for which weather will be reported
    * @param impact
    */

  def processData(date: String, lotr_list: List[String], impact: Int) : Unit = {

    import spark.implicits._
    val out: DataFrame = lotr_list.map(city => {
      val temp_cond = Elements.getTemperatureCondition(getDateString(date), LOTR_PLACES, city, impact)
      val location = Elements.getLocationData(city)
      val pres =  Elements.getPressure(temp_cond._1.value, location.elevation)
      val humid = Elements.setHumidity()
      (
      city,
      location.latitude,
        location.longitude,
        location.elevation,
        location.elev_unit,
      Elements.toTADate(date),
      temp_cond._2,
        temp_cond._1.value,
        temp_cond._1.unit,
        pres.value,
        pres.unit,
      humid.value,
        humid.unit
    )}).
    map({case (f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13) =>
          FinalRec(f1, (f"$f2%1.2f", f"$f3%1.2f", f"$f4%1.2f $f5"), f6.value, f7.value, f"$f8%1.2f $f9", f"$f10%1.2f $f11", f"$f12%1.2f $f13")}).toDF
    out.show()
    out.rdd.map(x => x.mkString("|")).
      map(_.replaceAll("[\\[\\]]", "")).
      saveAsTextFile("out/LOTRWeather.txt")

  }
}
