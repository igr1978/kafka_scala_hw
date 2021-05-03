name := "kafka_scala_hw"

version := "0.1"

scalaVersion := "2.12.13"
val jacksonVersion = "2.12.3"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.8.0",
  "org.json4s" %% "json4s-native" % "3.7.0-M16",
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-csv" % jacksonVersion
)