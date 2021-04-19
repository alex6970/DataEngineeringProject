import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark

case class Report(  // all parameters are public and immutable by default
                    val PeacewatcherId: Int,
                    val Latitude: Double,
                    val Longitude: Double,
                    val Country: String,
                    val NameCitizen: String,
                    val Words: String,
                    val Battery: Int,
                    val Age: Int,
                    val Alert: Int
                 )

object Producer {
  def main(args: Array[String]): Unit = {
    writeToKafka("topic_reports")
  }


  def writeToKafka(topic: String): Unit = {

    val pathToDataset = "src/main/resources/DataEng.csv"

    val conf = new SparkConf()
      .setAppName("Producer")
      .setMaster("local[*]") // here local mode. And * means you will use as much as you have cores.

    val sessionBuild = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    sessionBuild.read.csv(pathToDataset)

    val df = sessionBuild.read.option("header",true)
      .option("delimiter",";")
      .option("inferSchema", "true")
      .csv(pathToDataset)
      .toDF("PeacewatcherId", "Latitude", "Longitude", "Country", "NameCitizen", "Words", "Battery", "Age", "Alert")

    //df.foreach(line => println(line))

    //val alertes = "Alerts"

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("producer.config", "config/producer.properties")
    props.put("acks","all")


    val thread = new Thread{
    val producer = new KafkaProducer[String, String](props)

      // Reports->topic
    df.foreach{
      line => producer.send(new ProducerRecord(topic, "key", line.toString()))
        println(line)
        Thread.sleep(60000)
    }
      producer.close()

    }
    thread.start()

  }
}
