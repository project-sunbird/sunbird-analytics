package org.ekstep.analytics.streaming

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

    lazy val producer = createProducer()

    def send(topic: String, value: String): Unit = {
        val f = producer.send(new ProducerRecord(topic, value));
        f.get;
    }
    def close(): Unit = producer.close();
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