package bigdata

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

import java.util.Properties
import scala.io.Source.fromFile

object TripProducer extends App  {

  val topicName = "winter2020_iuri_trip_v4"
  val producerProperties = new Properties()
  producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
  producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer = new KafkaProducer[Int, String](producerProperties)

  val tripSource = "src/main/resources/100_trips.csv"

  var tripKey = 1
  val tripMessage = fromFile(tripSource).getLines().toList
    .foreach(line => {
      println(line)
      producer.send(new ProducerRecord[Int, String](topicName, tripKey, line))
      tripKey = tripKey + 1
    })
  producer.flush()

}
