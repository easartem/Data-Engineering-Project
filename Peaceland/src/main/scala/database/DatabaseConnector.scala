package database

import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

object DatabaseConnector extends App{

  Logger.getLogger("org").setLevel(Level.OFF) // vire les logs

  /** MongoDB Connection */
  val spark = SparkSession.builder()
    .master("local")
    .appName("MongoSparkConnectorIntro")
    .config("spark.mongodb.input.uri", "mongodb+srv://admin:admin@peaceland.ps95v.mongodb.net/")
    .config("spark.mongodb.input.database", "data_peaceland")
    .config("spark.mongodb.input.collection", "report")
    .getOrCreate()

  val sc = spark.sparkContext
  val df = MongoSpark.load(spark)
  //val rdd = MongoSpark.load(sc)

  // Print the number of report in the Database
  println("The number of drone's report : " + df.count())

  // Print the average peacescore
  println("The average peacescore is : ")
  df.select(mean(df("score")))
    .show()

  // Print the name of people with score higher than 80
  df.select(df("first_name"), df("last_name"), df("score"))
    .filter(df("score")>80)
    .show()

  // Print the worst citizens >:(
  println("This citizens have to go to peacecamp : ")
  df.select(df("first_name"), df("last_name"), df("score"))
    .orderBy(asc("score"))
    .show(5)

  // Find reports around a location
  df.select(df("last_name"), df("first_name"), df("score"), df("lat"), df("long"))
    .filter((df("lat") > -20.0000) && (df("lat") < 20.0000) && (df("long") > 100.0000) && (df("long") < 120.0000))
    .show()

  //println(rdd.first.toJson)
}
