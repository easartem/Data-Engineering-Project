package consumer

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}
import utils.EventUtils.readEvent

import java.time.Duration
import java.util.{Collections, Properties, Timer, TimerTask}
import scala.collection.JavaConversions.iterableAsScalaIterable

object EventConsumer extends App {
  /** --------------------Consumer config------------------------------ **/

  val topic = "Peaceland-STORAGE"

  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer])
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-storage")

  val consumer = new KafkaConsumer[Int, String](props)
  consumer.subscribe(Collections.singletonList(topic))


  /** --------------------Consume records from poll-------------------- **/

  // Manage consumer consumption of poll and close it once over
  def manageConsumer(consumerLifetime: Int, pollDuration: Int): Unit = {
    if ( consumerLifetime != 0) {
      val polledRecords = consumer.poll(Duration.ofSeconds(pollDuration))
      if (!polledRecords.isEmpty) {
        println(s"Polled ${polledRecords.count()} records")
        polledRecords.foreach(record => {
          val event = readEvent(record.value()) //access with event.attribute
          println(s"| ${record.key()} |  ${record.value()} | ${record.partition()} | ${record.offset()} |")
        })}
      //sleep?
      manageConsumer(consumerLifetime - pollDuration, pollDuration)
    }
  }

  // Run consumer
  manageConsumer(30000,1)
  consumer.close()

}
