package consumer

import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer,IntegerDeserializer}
import java.time.Duration
import scala.collection.JavaConverters._

object StreamToStorage extends App {
  //def consumeFromKafka(topic: String) = {

    val topic = "Peaceland-ALERT"
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer])
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group")

    val consumer = new KafkaConsumer[String, Integer](props)
    consumer.subscribe(List(topic).asJava)

    while (true) { //replace while by recursivity with foreach
      val polledRecords: ConsumerRecords[String, Integer] = consumer.poll(Duration.ofSeconds(1))
      if (!polledRecords.isEmpty) {
        println(s"Polled ${polledRecords.count()} records")
        val recordIterator = polledRecords.iterator()
        while (recordIterator.hasNext) {
          val record: ConsumerRecord[String, Integer] = recordIterator.next()
          println(s"| ${record.key()} | ${record.value()} | ${record.partition()} | ${record.offset()} |")
        }
      }
    }
}