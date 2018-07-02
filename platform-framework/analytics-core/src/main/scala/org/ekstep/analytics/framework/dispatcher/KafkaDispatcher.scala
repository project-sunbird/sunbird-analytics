package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.streaming.KafkaEventProducer
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.Level._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.streaming.KafkaSink
import java.util.HashMap
import java.lang.Long

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * @author Santhosh
 */
object KafkaDispatcher extends IDispatcher {

    implicit val className = "org.ekstep.analytics.framework.dispatcher.KafkaDispatcher"

    @throws(classOf[DispatcherException])
    def dispatch(events: Array[String], config: Map[String, AnyRef]): Array[String] = {
        val brokerList = config.getOrElse("brokerList", null).asInstanceOf[String];
        val topic = config.getOrElse("topic", null).asInstanceOf[String];
        if (null == brokerList) {
            throw new DispatcherException("brokerList parameter is required to send output to kafka")
        }
        if (null == topic) {
            throw new DispatcherException("topic parameter is required to send output to kafka")
        }
        KafkaEventProducer.sendEvents(events, topic, brokerList)
        events
    }

    def dispatch(config: Map[String, AnyRef], events: RDD[String])(implicit sc: SparkContext) = {
        val brokerList = config.getOrElse("brokerList", null).asInstanceOf[String]
        val topic = config.getOrElse("topic", null).asInstanceOf[String]
        if (null == brokerList) {
            throw new DispatcherException("brokerList parameter is required to send output to kafka")
        }
        if (null == topic) {
            throw new DispatcherException("topic parameter is required to send output to kafka")
        }

        events.foreachPartition((partitions: Iterator[String]) => {
            val kafkaSink = KafkaSink(_getKafkaProducerConfig(brokerList));
            partitions.foreach { message =>
                try {
                    kafkaSink.send(topic, message, new Callback {
                        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
                            if (null != exception) {
                                JobLogger.log(exception.getMessage, None, ERROR);
                            } else {
                                
                            }
                        }
                    })
                }
                catch {
                    case e: Exception =>
                        Console.println("SerializationException inside kafka dispatcher", e.getMessage);
                        JobLogger.log(e.getMessage, None, ERROR)
                }
            };
            kafkaSink.flush();
            kafkaSink.close()
        });
        
    }

    private def _getKafkaProducerConfig(brokerList: String): HashMap[String, Object] = {
        val props = new HashMap[String, Object]()
        props.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, 3000L.asInstanceOf[Long])
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props
    }

}
