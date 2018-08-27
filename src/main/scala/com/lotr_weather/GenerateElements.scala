package com.lotr_weather

import com.lotr_weather.Elements._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}

import scala.annotation.tailrec

object GenerateElements extends LotrSparkInit {


  case class FinalRec(place: Place,
                      location: Location,
                      date: ThirdAgeDate,
                      condition: Condition,
                      temperature: Temperature,
                      pressure: Pressure,
                      humidity: Humidity
                     ) {}

  final val LOTR_PLACES = LotrDetails.loadWeatherMapData(sc, LOTR_LOCATION_MAP)
  final val LIST_CITIES: List[String] = LOTR_PLACES.select("place").rdd.map(f => f(0).toString).collect().toList

  def processData(date: String, lotr_list: List[String]) : Unit = {
    lotr_list.indices.foreach(i => {
      val i_place = LIST_CITIES(i)
      val place = Place(i_place)
      val location = Elements.getLocationData(i_place)
      val i_date: ThirdAgeDate = Elements.toTADate(date)
      val temp_cond = Elements.getTemperatureCondition(getDateString(date), LOTR_PLACES, i_place)
      val condition = temp_cond._2
      val temperature = temp_cond._1
      val pressure = Elements.getPressure(temperature.value, location.elevation)
      val humidity = Elements.setHumidity()
      val final_rec = FinalRec(place, location, i_date, condition, temperature, pressure, humidity)

      println(final_rec.place.value + "|" +
        final_rec.location.longitude + ", " + final_rec.location.latitude + ", " + final_rec.location.elevation + "|" +
        final_rec.date.value + "|" +
        final_rec.condition.value + "|" +
        final_rec.temperature.value + " " + final_rec.temperature.unit + "|" +
        final_rec.pressure.value + " " + final_rec.pressure.unit + "|" +
        final_rec.humidity.value + " " + final_rec.humidity.unit
      )

    })
  }
}

/**
    val out: DataFrame = LIST_CITIES.map(city => {
      val place = Place(city)
      val location = Elements.getLocationData(city)
      val i_date: ThirdAgeDate = Elements.toTADate(date)
      val temp_cond = Elements.getTemperatureCondition(getDateString(date), LOTR_PLACES, city)
      val condition = temp_cond._2
      val temperature = temp_cond._1
      val pressure = Elements.getPressure(temperature.value, location.elevation)
      val humidity = Elements.getHumidity()
      import spark.implicits._
      val final_rec = Seq(FinalRec(place, location, i_date, condition, temperature, pressure, humidity)).
    }
      final_rec
    )
    out
      //write.mode("overwrite").format("csv").option("delimiter", "|").save("out/LotrWeather.txt")

  }
}
**/

    /**
    //@tailrec
    def tailtest (date: String, lotr_list: List[String], df: Dataset[Row]): DataFrame = {
      if (lotr_list.isEmpty) df.toDF()
      else {
        val i_place = lotr_list.head.toString
        val place : Place = Place(i_place)
        val location = Elements.getLocationData(i_place)
        val i_date: ThirdAgeDate = Elements.toTADate(date)
        val temp_cond = Elements.getTemperatureCondition(getDateString(date), LOTR_PLACES)
        val condition = temp_cond._2
        val temperature = temp_cond._1
        val pressure = Elements.getPressure(temperature.value, location.elevation)
        val humidity = Elements.getHumidity()
        import spark.implicits._
        val final_rec = List(FinalRec(place, location, i_date, condition, temperature, pressure, humidity)).
          map(f => (f.place.value.toString,
          (f.location.latitude, f.location.longitude, f.location.elevation),
          f.date.value.toString,
          f.condition.value.toString,
          f.temperature.value,
          f.pressure.value,
          f.humidity.value
          )).toDF()
        tailtest(date, lotr_list.tail, df.union(final_rec))
      }
    }
    import spark.implicits._
    val schema = StructType(StructField(_1,StringType,true), StructField(_2,StructType(StructField(_1,DoubleType,false), StructField(_2,DoubleType,false), StructField(_3,DoubleType,false)),true), StructField(_3,StringType,true), StructField(_4,StringType,true), StructField(_5,DoubleType,false), StructField(_6,DoubleType,false), StructField(_7,DoubleType,false))
    val resultingDF = tailtest(date, LIST_CITIES, sc.createDataFrame(emptyRDD[Row], schema)
    resultingDF.write.mode("overwrite").format("csv").option("delimiter", "|").save("out/LotrWeather.txt")
    resultingDF.show()
**/
