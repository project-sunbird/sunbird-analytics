package org.ekstep.analytics.model

import org.ekstep.analytics.util.DerivedEvent
import org.ekstep.analytics.framework.IBatchModel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.commons.beanutils.PropertyUtils
import org.ekstep.analytics.framework.Event

object EventFieldExtractor extends IBatchModel[Event, String] with Serializable {

    def execute(events: RDD[Event], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        val config = jobParams.getOrElse(Map());

        val keys = config.getOrElse("fields", "eid").asInstanceOf[String];
        val headers = config.getOrElse("headers", keys).asInstanceOf[String];
        val fields = keys.split(",");

        val headerRDD = sc.parallelize(Array(headers), 1)
        val valuesRDD = events.map { x =>
            for (f <- fields) yield PropertyUtils.getProperty(x, f);
        }.map(_.mkString(","));
        headerRDD.union(valuesRDD);
    }

}

object DerivedEventFieldExtractor extends IBatchModel[DerivedEvent, String] with Serializable {

    def execute(events: RDD[DerivedEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        val config = jobParams.getOrElse(Map());

        val keys = config.getOrElse("fields", "eid").asInstanceOf[String];
        val headers = config.getOrElse("headers", keys).asInstanceOf[String];
        val fields = keys.split(",");

        val headerRDD = sc.parallelize(Array(headers), 1)
        val valuesRDD = events.map { x =>
            for (f <- fields) yield JSONUtils.serialize(PropertyUtils.getProperty(x, f));
        }.map(_.mkString(","));
        headerRDD.union(valuesRDD);
    }

}