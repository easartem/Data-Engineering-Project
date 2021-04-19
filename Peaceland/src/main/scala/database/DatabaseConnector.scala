package database

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql.toMongoDataFrameReaderFunctions
import org.bson.Document

object DatabaseConnector {

  /** MongoDB Connection */
  val spark = SparkSession.builder()
    .master("local")
    .appName("MongoSparkConnectorIntro")
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")
    .getOrCreate()

  def writeMessage(db : String, topic : String, data : Array[(String, String)]): Unit ={

  //  val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
   // MongoSpark.save(documents)

    val col = List("key", "value")
    val df = spark.createDataFrame(data).toDF(col:_*)
    df.write.format("mongo").mode("append").save()
    spark.close()

  }

  def readMessage(db : String, topic : String) : Unit = {
    // Read the data from MongoDB to a DataFrame
    val readConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "zips")) // 1)
    val df = spark.read.mongo(readConfig)
    df.printSchema()
    df.show()
  }

}
