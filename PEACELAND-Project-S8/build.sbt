name := "PEACELAND-Project-S8"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5",
  "com.google.code.gson" % "gson" % "2.7",
  "org.apache.kafka" %% "kafka" % "0.8.2.1",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3",
  "org.apache.kafka" % "kafka-clients" % "2.3.1",
  "net.liftweb" %% "lift-json" % "2.6.2",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.1.0", //"2.1.0"  "4.2.3"
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.3.2"
)