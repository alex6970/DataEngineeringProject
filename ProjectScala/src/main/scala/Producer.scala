import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

case class Report(  // all parameters are public and immutable by default
                    val PeacewatcherId: Int,
                    val Latitude: Double,
                    val Longitude: Double,
                    val Country: String,
                    val NameCitizen: String,
                    val Words: String,
                    val Battery: Int,
                    val Age: Int,
                    val Alert: Boolean
                 )

object Producer {
  def main(args: Array[String]): Unit = {
    writeToKafka("topic_reports")
  }


  def writeToKafka(topic: String): Unit = {

    // Spark Session

    /*val conf = new SparkConf()
      .setAppName("Producer")
      .setMaster("local[*]") // here local mode. And * means you will use as much as you have cores.
    val sc = SparkContext.getOrCreate(conf)
     */

    val pathToDataset = "src/main/resources/DataEng.csv"


    val conf = new SparkConf()
      .setAppName("Producer")
      .setMaster("local[*]") // here local mode. And * means you will use as much as you have cores.

    val sessionBuild = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    sessionBuild.read.csv(pathToDataset)

    val df = sessionBuild.read.option("header",true)
      .csv(pathToDataset)

    df.show()


    /*val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, "key", "value")
    producer.send(record)
    producer.close()
     */
  }
}
