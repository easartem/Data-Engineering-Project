name := "Test"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5",
  "org.twitter4j" % "twitter4j-core" % "4.0.5",
  "com.google.code.gson" % "gson" % "2.7",
  "com.typesafe.akka" % "akka-actor_2.11" % "2.3.4",
  "org.apache.kafka" %% "kafka" % "0.8.2.1",
  "org.apache.kafka" % "kafka-clients" % "2.3.1"
)
