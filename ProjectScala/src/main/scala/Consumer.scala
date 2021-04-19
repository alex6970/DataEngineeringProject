import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConverters._

object Consumer {
  def main(args: Array[String]): Unit = {
    consumeFromKafka("topic_reports")
  }

  def consumeFromKafka(topic: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "key")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

    //MongoDB connection

    consumer.subscribe(util.Arrays.asList(topic))

    while (true) {

      val record = consumer.poll(1000).asScala

      record.foreach {
        data => data.value()
          //save data to MongoDB
      }

    }
  }
}