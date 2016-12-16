package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.Event
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework._
import org.joda.time.DateTime
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import java.io.File
import org.apache.hadoop.io.compress.GzipCodec
import com.datastax.spark.connector._

case class JobSummary(client_id: Option[String], request_id: Option[String], job_id: Option[String], status: Option[String], request_data: Option[String], config: Option[String],
                      locations: Option[List[String]], dt_file_created: Option[DateTime], dt_first_event: Option[DateTime], dt_last_event: Option[DateTime],
                      dt_expiration: Option[DateTime], iteration: Option[Int], dt_job_submitted: Option[DateTime], dt_job_processing: Option[DateTime],
                      dt_job_completed: Option[DateTime], input_events: Option[Int], output_events: Option[Int], file_size: Option[Long], latency: Option[Int],
                      execution_time: Option[Long], err_message: Option[String]) extends AlgoOutput

@scala.beans.BeanInfo
case class DataExhaustInput(tag: String, events: Buffer[Event]) extends AlgoInput
case class RequestOutput(request_id: String, output_events: Int)
                      
object DataExhaustModel extends IBatchModelTemplate[Event, DataExhaustInput, Empty, Empty] with Serializable {

    val className = "org.ekstep.analytics.model.DataExhaustModel"
    override def name: String = "DataExhaustModel"
    
    override def preProcess(data: RDD[Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DataExhaustInput] = {

        val request_id = config.getOrElse("request_id", "").asInstanceOf[String]
        sc.parallelize(Array((request_id, data.count().toInt))).saveToCassandra("general_db", "jobs", SomeColumns("request_id","input_events"))
        data.map { x =>
            val tagList = x.tags.asInstanceOf[List[Map[String, List[String]]]]
            val genieTagFilter = if (tagList.nonEmpty) tagList.filter(f => f.contains("genie")) else List()
            val tempList = if (genieTagFilter.nonEmpty) genieTagFilter.filter(f => f.contains("genie")).last.get("genie").get; else List();
            tempList.map { f =>
                (f,x)
            }
        }.flatMap(f => f).map { x => (x._1, Buffer(x._2)) }
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => DataExhaustInput(x._1, x._2) };
    }

    override def algorithm(data: RDD[DataExhaustInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {

        val filterTags = config.getOrElse("tags", List()).asInstanceOf[List[String]]
        val request_id = config.getOrElse("request_id", "").asInstanceOf[String]
        
        val dataMap = data.collect().map{x => (x.tag, x.events)}.toMap
        val outputEvents = filterTags.map{ f =>
            if(dataMap.contains(f)) dataMap.get(f).get else Buffer[Event]()
        }.flatMap { x => x }
        val distinctOutput = sc.parallelize(outputEvents).map{y => JSONUtils.serialize(y)}.distinct()//.map { x => JSONUtils.deserialize[Event](x) }
        sc.parallelize(Array((request_id, distinctOutput.count().toInt))).saveToCassandra("general_db", "jobs", SomeColumns("request_id","output_events"))
//        distinctOutput.saveAsTextFile("/tmp/data-exhaust/request_id", classOf[GzipCodec])
//        CommonUtil.gzip("/tmp/data-exhaust")
        sc.makeRDD(List(Empty()));
    }

    override def postProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {

        sc.makeRDD(List(Empty()));
    }
}