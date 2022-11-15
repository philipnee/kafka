package kafka.api

import kafka.utils.Implicits.PropertiesOps
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer}

import java.util.Properties
import scala.collection.mutable

class PlaintextAsyncConsumerTest extends PlaintextConsumerTest {
  val consumers = mutable.Buffer[KafkaConsumer[_, _]]()
  override def createConsumer[K, V](keyDeserializer: Deserializer[K] = new ByteArrayDeserializer,
                                    valueDeserializer: Deserializer[V] = new ByteArrayDeserializer,
                                    configOverrides: Properties = new Properties,
                                    configsToRemove: List[String] = List()): KafkaConsumer[K, V] = {
    val props = new Properties
    props ++= consumerConfig
    props ++= configOverrides
    configsToRemove.foreach(props.remove(_))
    // TODO: switch to prototype impl once basic consume is done
    val consumer = new KafkaConsumer[K, V](props, keyDeserializer, valueDeserializer)
    consumers += consumer
    consumer
  }
}