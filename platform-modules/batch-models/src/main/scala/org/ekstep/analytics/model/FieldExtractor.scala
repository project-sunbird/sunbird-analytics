package org.ekstep.analytics.model

import org.ekstep.analytics.util.DerivedEvent
import org.ekstep.analytics.framework.IBatchModel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.commons.beanutils.PropertyUtils
import org.ekstep.analytics.framework.Event
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.StringEscapeUtils

trait FieldExtractor extends Serializable {

    def serializeToCSV[T](events: RDD[T], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {
        val config = jobParams.getOrElse(Map());

        val keys = config.getOrElse("fields", "eid").asInstanceOf[String];
        val headers = config.getOrElse("headers", keys).asInstanceOf[String];
        val fields = keys.split(",");

        val headerRDD = sc.parallelize(Array(headers), 1)
        val valuesRDD = events.map { x =>
            for (f <- fields) yield getValue(x, f);
        };
        valuesRDD.collect();
        
        headerRDD.union(valuesRDD.map(_.mkString(",")));
    }
    
    def getValue[T](event: T, field: String) : String = {
        val propValue = PropertyUtils.getProperty(event, field);
        StringEscapeUtils.escapeCsv(JSONUtils.serialize(propValue));
    }
}

object EventFieldExtractor extends IBatchModel[Event, String] with FieldExtractor {

    def execute(events: RDD[Event], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {
        serializeToCSV[Event](events, jobParams);
    }
    
    def main(args: Array[String]): Unit = {
        val str = """[{"survey_codes":"aser007"},{"activation_keys":"ptm007"},{"partnerid":"org.ekstep.partner.pratham"}]""";
        println(JSONUtils.serialize(StringEscapeUtils.escapeCsv(str)));
        println(StringEscapeUtils.escapeJson(str));
    }
    
}

object DerivedEventFieldExtractor extends IBatchModel[DerivedEvent, String] with FieldExtractor {

    def execute(events: RDD[DerivedEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {
        serializeToCSV[DerivedEvent](events, jobParams);
    }

}