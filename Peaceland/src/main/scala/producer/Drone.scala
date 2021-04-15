package producer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.ReportUtils
import utils.ReportUtils.Report

import scala.concurrent.duration._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
//import org.apache.kafka.clients.producer.ProducerRecord

/** Each drone generate a report every minute ! We have to :
 *    - generate random data (id, current location lat/long, citizens' name and peacescore, surroundings conversations
 *    - instanciate kafka producer stream
 *    - send message containing the report each minute */

class Drone(val id: Int){

  /** ATTRIBUTES **/
  val rand = scala.util.Random
  val pathToFile = "data/random_report.json"
  val props: Properties = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  val producer = new KafkaProducer[String, String](props)


  /** METHODS **/

  //Load the data from the json file and return an RDD of Report
  def loadData(): RDD[Report] = {
    // create spark configuration and spark context
    val conf = new SparkConf()
      .setAppName("Random Report")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    sc.textFile(pathToFile)
      .mapPartitions(ReportUtils.parseFromJson)
  }

  //Select random Report from the RDD
  def generateReport(): RDD[Report] = {
    val random_line = rand.nextInt(100)
    loadData().filter(r => r.id == random_line)
  }

  def writeToKafka(topic: String): Unit = {
    val record = new ProducerRecord[String, String](topic, "key", "value")
    producer.send(record)
    println("Drone produced a record:")
  }

  def sendReport(iter: Int): Any = {
    //val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
    val deadline = iter.seconds.fromNow
    while (deadline.hasTimeLeft)
    {
      writeToKafka("Test")
      Thread.sleep(2000)
    }
  }

      /**
  // generate random data and sends message every minute
  def sendReport(): Unit = {
    println(s"Random number is ${rand.nextInt(100)}")
  }**/
}
