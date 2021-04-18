package producer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.EventUtils
import utils.EventUtils.Event

//rdd.collect().foreach(println)
/** Each drone generate a report every minute ! We have to :
 *    - generate random data (id, current location lat/long, citizens' name and peacescore, surroundings conversations
 *    - instanciate kafka producer stream
 *    - send message containing the report each minute */


class Drone(val id: Int) extends AlertRecordTrait {

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


  /** Status : TO MODIFY (Add alertProducer & send alertProducer) */
  def handleEvent(rdd: Event): Unit = rdd.score match {
    // Good citizen, DO NOTHING
    case rdd.score if (rdd.score >= 50 && rdd.score <= 100) => {
      println(s"Citizen ok with score ${rdd.score}")
    }
    // Bad citizen, PRODUCE ALERT WITH KAFKA
    case rdd.score if (rdd.score >= 0 && rdd.score < 50) => {
      println(s"ALERT : Citizen instable with score ${rdd.score}")
      sendAlertRecord(rdd)
    }
    // Wrong argument
    case _ => println("Error")  }


  /** Status : TO DO */
  def sendEvent(event : Event, makeReport : Boolean): Unit = (
    // We need to send a boolean to the topic in order to indicate the last event of a report
    println("Send EventRecord with EventProducer")
  )


  /** Status : TO MODIFY */
  def simulateDrone(i:Int) : Unit = i match {
    // Start of the drone simulation
    case 1 => {
      println("Begin drone simulation...")
      println("")
      println(s"New Event, ${i}")
      val rdd = generateEvent().first()
      handleEvent(rdd)
      sendEvent(rdd,false)
      Thread.sleep(1000)
      simulateDrone(i+1)
    }
    // End of the drone simulation
    case 40 => {
      println("Generate Report, reset i to 0")
      println("This is the END")
    }
    // 1 minute of events was generated so we pass our intention to generate a report to the eventConsumer
    case i if (i%20 == 0) => {
      println("")
      val rdd = generateEvent().first()
      handleEvent(rdd)
      sendEvent(rdd,true)
      println(s"New Event, ${i}")
      println("Generate Report")
      Thread.sleep(1000)
      simulateDrone(i+1)
    }
    // Generate event every 3 seconds
    case _ => {
      println("")
      println(s"New Event, ${i}")
      val rdd = generateEvent().first()
      handleEvent(rdd)
      sendEvent(rdd,false)
      Thread.sleep(1000)
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
