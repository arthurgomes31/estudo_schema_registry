package bigdata

import scala.io.Source

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema

object SchemaRegistry extends App {

  val schemaSource = "src/main/resources/enrichedtrip.avsc"

  val schemaFile = Source.fromFile(schemaSource).getLines().mkString

  val enrichedTripSchema = new Schema.Parser().parse(schemaFile)

  val srClient = new CachedSchemaRegistryClient("http://localhost:8081", 1)
  srClient.register("winter2020_iuri_enriched_trip_v4-value", enrichedTripSchema)

}

