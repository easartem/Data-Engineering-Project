package producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import producer.`trait`.AlertRecordTrait
import utils.EventUtils
import utils.EventUtils.Event

import java.util.Properties

//rdd.collect().foreach(println)
/** Each drone generate a report every minute ! We have to :
 *    - generate random data (id, current location lat/long, citizens' name and peacescore, surroundings conversations
 *    - instanciate kafka producer stream
 *    - send message containing the report each minute */

//class Drone(val id: Int) extends AlertRecordTrait {
class Drone(val id: Int)  {

  /** --------------------ATTRIBUTES------------------------------ **/
  val rand = scala.util.Random
  val pathToFile = "data/random_event.json"

  /** --------------------Kafka Alert Producer-------------------- **/

  // KAFKA PRODUCER CONFIG
  val props: Properties = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

  // KAFKA PRODUCER INSTANCE
  val producerAlert: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  // KAFKA PRODUCER METHOD (produce record and send it to topic)
  def writeToKafka(topic: String, name : String, location : String): Unit = {
    val record = new ProducerRecord[String, String](topic, name, location)
    println(" checkpoint")
    producerAlert.send(record)
    println(s"[$topic] Drone sent alert for #${name} to location ${location}")
  }

  /** --------------------METHODS--------------------------------- **/


  /** Status : ok */
  //Load the data from the json file and return an RDD of Event
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
  //Select a random event from the json and return an RDD of Event
  def generateEvent() : RDD[Event] = {
    val random_line = rand.nextInt(500) + 1
    loadData().filter(r => r.row == random_line)//.first() to get an Event instead of a RDD[Event]
  }


  /** Status : ok */
  //Define the drone behaviour according to citizen peacescore
  def handleEvent(rdd: Event): Unit = rdd.score match {
    // Good citizen, DO NOTHING
    case rdd.score if (rdd.score >= 50 && rdd.score <= 100) => {
      println(s"Citizen ok with score ${rdd.score}")
    }
    // Bad citizen, PRODUCE ALERT WITH KAFKA
    case rdd.score if (rdd.score >= 0 && rdd.score < 50) => {
      println(s"ALERT : Citizen instable with score ${rdd.score}")
      val name = rdd.first_name + " " + rdd.last_name
      val location = "(" + rdd.lat.toString + "," + rdd.long.toString + ")"
      writeToKafka("Peaceland-ALERT", name, location)
    }
    // Wrong argument
    case _ => println("Error")  }


  /** Status : TO DO */
  def sendEvent(event : Event, makeReport : Boolean): Unit = (
    // We need to send a boolean to the topic in order to indicate the last event of a report
    println("Send EventRecord with EventProducer")
  )

  /** Status : ok */
  //Event lifecycle (create an event, handle the event peacescore, record the event)
  def processEvent(i:Int, reportStatus:Boolean): Unit = {
    println(s"\nNew Event, ${i}")
    val rdd = generateEvent().first()
    handleEvent(rdd)
    sendEvent(rdd,reportStatus)
    Thread.sleep(1000)
  }

  /** Status : TO MODIFY */
  //Simulate a drone who produce events every 3 sec and send a report every minute
  def simulateDrone(i:Int) : Unit = i match {
    // Start of the drone simulation
    case 1 => {
      println("Begin drone simulation...")
      processEvent(i,reportStatus = false)
      simulateDrone(i+1)
    }
    // End of the drone simulation
    case 25 => {
      println("This is the END")
    }
    // 1 minute of events was generated so we pass our intention to generate a report to the eventConsumer
    case i if (i%20 == 0) => {
      processEvent(i,reportStatus = true)
      println("Generate Report")
      simulateDrone(i+1)
    }
    // Generate event every 3 seconds
    case _ => {
      processEvent(i,reportStatus = false)
      simulateDrone(i+1)
    }
  }
}

/**
 *                      _-AlertProducer ----> AlertConsumer
 * handleEvent :  Event
 *                      -_EventProducer ----> EventConsumer
 *                                              if (makeReport == True)
 *                                                  Make Report (??)
 *                                                  ReportProducer ------> ReportConsumer
 *                                              else
 *                                                  store event (??)
 *
 *     EventProducer needs recordEvent with boolean value True or False for report stop condition
 */