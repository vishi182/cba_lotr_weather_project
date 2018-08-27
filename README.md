# cba_lotr_weather_project

A fun implementation of CBA's weather report project implemented for the world of JRR Tolkein's Lord of the Rings.
It assumes that the main villain of the book - Sauron has won the war, which leads to global warming of the planet. 

Abbreviations
- LOTR - Lord of the Rings
- T.A. - Third Age. Weather report dates are preceded by T.A. 

Technology Stack
- Scala version 2.11.9
- Apache Spark 2.3.1

Output format
- Output report will be printed on the terminal, as well as stored at ~LOCAL_DIR/out/LOTRWeather.txt
- Sample record from the report

Hobbiton|51.75,-1.26,35.00 mts|T.A.3020-09-20 00:16:00|Rain|15.36 Celsius|100779.34 hPa|80.00 Percent


Input Files
- 2015_copy.txt: NCDC weather record file for the year 2015. It has temperature, precipitation, snow etc data of weather stations of the world. The actual file is 1GB but I have posted a smaller file for our purposes.
- ghcnd-stations.txt: Weather station data. With Weather Station Id, name, coordinates etc
- lotr_map.txt: File containing LOTR city name, coordinates, elevation and real world city name mapped to LOTR city. 
    
Key Features
- Uses Apache Spark to read actual weather data file from NCDC to generate weather data for Lord of the Rings' Middle Earth Cities.
- Envisages that Sauron's activities will result in global warming of the world. Temperature of the cities keep progressively increasing after T.A. 23 Sep 3018.
    
Run with random date picked by the program
- sbt run
- sbt "run-main com.lotr_weather.WeatherGenerator"

Run by passing a desired date. Date format will be "YYYYMMDD"
- sbt "run-main com.lotr_weather.WeatherGenerator" "30190323"  

Open the sbt project using your favorite IDE and run the WeatherGenerator.


TODO
- Implement rule engine to reduce hard coding and make design more flexible
- Research Machine Learning algorithms, where this project could be modeled and improved
            