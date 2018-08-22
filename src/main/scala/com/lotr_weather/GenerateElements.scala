package com.lotr_weather

case class GenerateElements(sr_num: Int,
                            place: String,
                            location: Location,
                            condition: Condition,
                            temperature: Temperature,
                            pressure: Pressure,
                            humidity: Humidity) {}
