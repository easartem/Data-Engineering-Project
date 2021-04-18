package producer

import org.apache.kafka.clients.producer.ProducerRecord
import utils.EventUtils.Event

/** for the sake of testing, replace AlertData by Event
 * Record with {key : name} and {value : peacescore}
 * NB : In reality, alertData should provide citizen name + drone location
 */

trait AlertRecordTrait extends ProducerTrait {
  val topic = "Peaceland-ALERT"

  def createAlertRecord(myAlertData: Event): ProducerRecord[String, Integer] = {
    //ProducerRecord(String topic, K key, V value)
    //Cannot resolve overloaded constructor `ProducerRecord[String, Event]`--> need to
    new ProducerRecord[String, Integer](
      topic,
      myAlertData.first_name,
      myAlertData.score
    )
  }

  def sendAlertRecord(myAlertData: Event): Unit = {
    producerAlert.send(createAlertRecord(myAlertData))
    println(s"[$topic] Drone sent alert #${myAlertData.first_name} to location (${myAlertData.lat}, ${myAlertData.long})")
  }
}
