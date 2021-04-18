package producer

import org.apache.kafka.clients.producer.KafkaProducer
import utils.EventUtils.Event
import utils.ReportUtils.Report
import java.util.Properties

/** Interface for kafka producers */

trait ProducerTrait {

  /**----Kafka Producer Server Config----*/

  private val bootstrapServers = "163.172.191.74:9092"
  private val props = new Properties()
  props.put("bootstrap.servers", bootstrapServers)


  /**----Kafka Producer Config for the Alert System----*/

  def producerAlert: KafkaProducer[String, Integer] = { // Replace Event by a class for Alert Data
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // props.put("value.serializer", "serialization.GenericSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")

    val producerAlertInstance = new KafkaProducer[String, Integer](props) // Replace Event by a class for Alert Data
    producerAlertInstance
  }

  /**----Kafka Producer Config for the Storage System----*/

  def producerStorage: KafkaProducer[Integer, Report] = {
    props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
    //props.put("value.serializer", "prestacop.serialization.GenericSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producerStorageInstance = new KafkaProducer[Integer, Report](props)
    producerStorageInstance
  }
}
