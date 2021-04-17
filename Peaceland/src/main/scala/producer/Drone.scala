package producer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.EventUtils
import utils.EventUtils.Event
import scala.concurrent.duration._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties


//rdd.collect().foreach(println)
/** Each drone generate a report every minute ! We have to :
 *    - generate random data (id, current location lat/long, citizens' name and peacescore, surroundings conversations
 *    - instanciate kafka producer stream
 *    - send message containing the report each minute */

class Drone(val id: Int){

  /** --------------------ATTRIBUTES-------------------- **/
  val rand = scala.util.Random
  val pathToFile = "data/random_event.json"


  /** --------------------METHODS----------------------- **/

  /** Status : ok */
  //Load the data from the json file and return an RDD of Report
  def loadData(): RDD[Event] = {
    // create spark configuration and spark context
    val conf = new SparkConf()
      .setAppName("Random Report")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    sc.textFile(pathToFile)
      .mapPartitions(EventUtils.parseFromJson)
  }

  /** Status : ok */
  //Select random line from the json through RDD
  def generateEvent() : RDD[Event] = {
    val random_line = rand.nextInt(100)
    loadData().filter(r => r.row == random_line)//.first()
    // We can add .first to get an Event instead of a RDD[Event]
    // But we need to watch for Exception Caused by: java.lang.UnsupportedOperationException: empty collection
  }

  /** Status : TO MODIFY */
  def handleEvent(peacescore: Int): Unit = peacescore match {
    case peacescore if (peacescore >= 50 && peacescore <= 100) => println(s"Citizen ok with score ${peacescore}") // DO NOTHING
    case peacescore if (peacescore >= 0 && peacescore < 50) => println(s"ALERT : Citizen instable with score ${peacescore}") // PRODUCE ALERT WITH KAFKA
    case _ => println("Error")  }

  /** Status : TO DO */
  //Return 1 min of report containing a List of events, droneID and surrounding conversations
  def generateReport(): Unit = {
    print("To modify")
  }

  /** Status : in testing */
  def simulateDrone(iter: Int): Unit = {
    println("Begin drone simulation...")
    val deadline = iter.seconds.fromNow
    while (deadline.hasTimeLeft)
    {
      val rdd = generateEvent()
      generateEvent()
      handleEvent(rdd.first().score)
      // Need to generate a report containing all the events of the last minute
      // But can't add event to list as it is an immutable data structure, like a Java String
      // Then produce a record containing the report
      // Every minute, sends the record with kafka producer
      Thread.sleep(2000)
    }
  }

  /** --------------------KAFKA WARZONE----------------- **/
  /**

  // Kafka is correctly linked to the project
  // The simple example below works correctly

  // KAFKA PRODUCER CONFIG
  val props: Properties = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  // KAFKA PRODUCER FOR TESTING DEPENDENCIES
  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
  // TEST
  def writeToKafka(topic: String): Unit = {
    val record = new ProducerRecord[String, String](topic, "key", "value")
    producer.send(record)
    println("Drone produced a record: (key,value")
  }

  // For our project, we need 2 producers alert and storage with different config and custom serializer or json serializer
  // producerAlertInstance = new KafkaProducer[Integer, AlertData](props)
  // producerStorageInstance = new KafkaProducer[Integer, Report](props)
   */
}
