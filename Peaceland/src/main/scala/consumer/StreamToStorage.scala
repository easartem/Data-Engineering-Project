package consumer


import java.util
import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}

import java.time.Duration
import scala.collection.JavaConverters._

object StreamToStorage {
  //def consumeFromKafka(topic: String) = {
  def main(args: Array[String]): Unit = {
    consumeFromKafka("Test")
  }

  def consumeFromKafka(topic: String) = {
    //val topic = "Test"
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(List(topic).asJava)

    while (true) {
      val polledRecords: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(1))
      if (!polledRecords.isEmpty) {
        println(s"Polled ${polledRecords.count()} records")
        val recordIterator = polledRecords.iterator()
        while (recordIterator.hasNext) {
          val record: ConsumerRecord[String, String] = recordIterator.next()
          println(s"| ${record.key()} | ${record.value()} | ${record.partition()} | ${record.offset()} |")
        }
      }
    }
  }
}