package org.ekstep.analytics.streaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

    lazy val producer = createProducer()

    def send(topic: String, value: String): Future[RecordMetadata] = Future {
        producer.send(new ProducerRecord(topic, value)).get
    }

    def close(): Unit = producer.close()
}

object KafkaSink {
    def apply(config: java.util.Map[String, Object]): KafkaSink = {
        val f = () => {
            val producer = new KafkaProducer[String, String](config)
            sys.addShutdownHook {
                producer.close()
            }
            producer
        }
        new KafkaSink(f)
    }
}