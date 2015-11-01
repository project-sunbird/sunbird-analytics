package org.ekstep.ilimi.analytics.framework

import org.apache.spark.rdd.RDD
import org.ekstep.ilimi.analytics.framework.exception.DispatcherException
import org.ekstep.ilimi.analytics.framework.dispatcher.S3Dispatcher
import org.ekstep.ilimi.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.ilimi.analytics.framework.dispatcher.ScriptDispatcher

/**
 * @author Santhosh
 */
object OutputDispatcher {

    def dispatch(outputs: Option[Array[Dispatcher]], events: RDD[String]) = {
        val eventArr = events.collect();
        if (outputs.isEmpty) {
            throw new DispatcherException("No output configurations found");
        }
        if (eventArr.size == 0) {
            throw new DispatcherException("No events produced for output dispatch");
        }
        outputs.get.foreach { dispatcher =>
            dispatcher.to.toLowerCase() match {
                case "s3" =>
                    val filePath = dispatcher.params.getOrElse("filePath", null).asInstanceOf[String];
                    val bucket = dispatcher.params.getOrElse("bucket", null).asInstanceOf[String];
                    val key = dispatcher.params.getOrElse("key", null).asInstanceOf[String];
                    val isPublic = dispatcher.params.getOrElse("public", null).asInstanceOf[Boolean];
                    if (null != filePath) {
                        S3Dispatcher.outputToS3(bucket, isPublic, key, filePath);
                    } else {
                        S3Dispatcher.outputToS3(bucket, isPublic, key, eventArr)
                    }
                case "kafka" =>
                    val brokerList = dispatcher.params.getOrElse("brokerList", null).asInstanceOf[String];
                    val topic = dispatcher.params.getOrElse("topic", null).asInstanceOf[String];
                    KafkaDispatcher.outputToKafka(brokerList, topic, eventArr);
                case "script" =>
                    val script = dispatcher.params.getOrElse("script", null).asInstanceOf[String];
                    val envVariables = dispatcher.params.map(f => (f._1, f._2.asInstanceOf[String]));
                    ScriptDispatcher.outputToScript(script, envVariables, eventArr);
                case _ =>
                    throw new DispatcherException("Unknown output configuration found");
            }
        }
    }
}