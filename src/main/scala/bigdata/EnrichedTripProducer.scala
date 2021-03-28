package bigdata

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties

object EnrichedTripProducer extends App {

  val spark = SparkSession.builder().appName("Sprint 3 Project V4").master("local[*]").getOrCreate()

  val fileEnrichedStation = "src/main/resources/EnrichedStation.csv"
  val stationInformationRdd = spark.sparkContext.textFile(fileEnrichedStation).map(fromCsv => StationsInfo(fromCsv))
  import spark.implicits._
  val stationInformationDf = stationInformationRdd.toDF()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
  val kafkaConfig = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.GROUP_ID_CONFIG -> "group-id-100-trips-records",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")
  val topic = "winter2020_iuri_trip_v4"
  val inStream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](List(topic), kafkaConfig))
  inStream.map(_.value()).foreachRDD(rdd => businessLogic(rdd))

  val producerProperties = new Properties()
  producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
  producerProperties.setProperty("schema.registry.url", "http://localhost:8081")

  def rowToAvro(row: Row, enrichedTripSchema: Schema): GenericRecord = {
    new GenericRecordBuilder(enrichedTripSchema)
      .set("start_date", row.getAs[String](0))
      .set("start_station_code", row.getAs[Int](1))
      .set("end_date", row.getAs[String](2))
      .set("end_station_code", row.getAs[Int](3))
      .set("duration_sec", row.getAs[Int](4))
      .set("is_member", row.getAs[Int](5))
      .set("system_id", row.getAs[String](6))
      .set("timezone", row.getAs[String](7))
      .set("station_id", row.getAs[Int](8))
      .set("name", row.getAs[String](9))
      .set("short_name", row.getAs[String](10))
      .set("lat", row.getAs[Double](11))
      .set("lon", row.getAs[Double](12))
      .set("capacity", row.getAs[Int](13))
      .build()
  }

  def businessLogic(rdd: RDD[String]) = {
    val trips: RDD[Trip] = rdd.map(fromCsv => Trip(fromCsv))
    val tripsDf = trips.toDF()
    tripsDf.createOrReplaceTempView("trip")
    stationInformationDf.createOrReplaceTempView("station")

    val joinResult = spark.sql(
      """
        |SELECT start_date, start_station_code, end_date, end_station_code, duration_sec, is_member,
        |system_id, timezone, station_id, name, short_name, lat, lon, capacity
        |FROM trip LEFT JOIN station ON trip.start_station_code = station.short_name
        |""".stripMargin)

    joinResult
      .rdd
      .foreachPartition { aPartition =>
        val producer = new KafkaProducer[String, GenericRecord](producerProperties)

        val srClient = new CachedSchemaRegistryClient("http://localhost:8081", 1)
        val metadata = srClient.getSchemaMetadata("winter2020_iuri_enriched_trip_v4-value", 2)
        val enrichedTripSchema = srClient.getByID(metadata.getId)

        aPartition
          .map(row => rowToAvro(row, enrichedTripSchema))
          .foreach { enrichedTripAvro =>
          val key = enrichedTripAvro.get("start_station_code").toString
          val record = new ProducerRecord[String, GenericRecord]("winter2020_iuri_enriched_trip_v4", key, enrichedTripAvro)
          producer.send(record)
          producer.flush()
        }
        producer.close()
      }
  }

  ssc.start()
  ssc.awaitTermination()
}
