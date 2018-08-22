name := "cba_lotr_weather_project"

version := "0.1"

scalaVersion := "2.11.9"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1",
  "joda-time" % "joda-time" % "2.10"
)
