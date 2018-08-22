package com.lotr_weather

/**
  * Historical record of weather stations
  *
  * @param stn_id
  * @param date
  * @param element
  * @param value
  * @param m_flag
  * @param q_flag
  * @param s_flag
  */

case class Records (stn_id: String,
                    date: String,
                    element: String,
                    value: String,
                    m_flag: String,
                    q_flag: String,
                    s_flag: String) {

}
