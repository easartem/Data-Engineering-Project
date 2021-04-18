package consumer

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.{Collections, Properties, Timer, TimerTask}
import scala.collection.JavaConversions.iterableAsScalaIterable

object AlertConsumer extends App {

  /** --------------------Consumer config------------------------------ **/

  val topic = "Peaceland-ALERT"

  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-alert")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(Collections.singletonList(topic))


  /** --------------------Consume records from poll-------------------- **/

  // Manage consumer consumption of poll and close it once over
  def manageConsumer(consumerLifetime: Int, pollDuration: Int): Unit = {
    if ( consumerLifetime != 0) {
      val polledRecords = consumer.poll(Duration.ofSeconds(pollDuration))
      if (!polledRecords.isEmpty) {
        println(s"Polled ${polledRecords.count()} records")
        polledRecords.foreach(record => {
          println(s"| ${record.key()} | ${record.value()} | ${record.partition()} | ${record.offset()} |")
        })}
      //sleep?
      manageConsumer(consumerLifetime - pollDuration, pollDuration)
    }
  }

  // Run consumer
  manageConsumer(30000,1)
  consumer.close()
}
