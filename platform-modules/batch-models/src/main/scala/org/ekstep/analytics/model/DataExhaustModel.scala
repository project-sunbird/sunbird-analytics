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
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.dispatcher.S3Dispatcher
import org.ekstep.analytics.framework.util.S3Util

case class JobSummary(client_id: Option[String], request_id: Option[String], job_id: Option[String], status: Option[String], request_data: Option[String], config: Option[String],
                      location: Option[String], dt_file_created: Option[DateTime], dt_first_event: Option[DateTime], dt_last_event: Option[DateTime],
                      dt_expiration: Option[DateTime], iteration: Option[Int], dt_job_submitted: Option[DateTime], dt_job_processing: Option[DateTime],
                      dt_job_completed: Option[DateTime], input_events: Option[Int], output_events: Option[Int], file_size: Option[Long], latency: Option[Int],
                      execution_time: Option[Long], err_message: Option[String]) extends AlgoOutput
case class DataExhaustInput(tag: String, events: Buffer[Event]) extends AlgoInput
                      
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
        val local_path = config.getOrElse("local_path", "mnt/data/analytics/data-exhaust/").asInstanceOf[String]
        val bucket = config.getOrElse("bucket", "ekstep-public").asInstanceOf[String]
        val key = config.getOrElse("key", "data-exhaust/").asInstanceOf[String]
        val days_for_expiration = config.getOrElse("days_for_expiration", 30).asInstanceOf[Int]
        
        val dataMap = data.collect().map{x => (x.tag, x.events)}.toMap
        val outputEvents = filterTags.map{ f =>
            if(dataMap.contains(f)) dataMap.get(f).get else Buffer[Event]()
        }.flatMap { x => x }
        val sortedEvents = outputEvents.sortBy { x => CommonUtil.getEventTS(x) }
        val firstEventDate = if(sortedEvents.size>0) CommonUtil.getEventTS(sortedEvents(0)) else None
        val lastEventDate = if(sortedEvents.size>0) CommonUtil.getEventTS(sortedEvents.last) else None
        val distinctOutput = sc.parallelize(outputEvents).map{y => JSONUtils.serialize(y)}.distinct()
        val output_events_count = distinctOutput.count()

        if(output_events_count > 0)
        {
            val file = new File(local_path+request_id)
            if (file.exists())
                CommonUtil.deleteDirectory(local_path+request_id)
            distinctOutput.saveAsTextFile(local_path+request_id)
            val files = sc.wholeTextFiles(local_path+request_id).map{x => x._1.split(":").last}
            CommonUtil.zip(local_path+request_id+".zip", files.collect())
            CommonUtil.deleteDirectory(local_path+request_id)
            S3Dispatcher.dispatch(Array(""), Map("filePath" -> (local_path+request_id+".zip"), "bucket" -> bucket, "key" -> (key+request_id+".zip")))
            val location = "s3n://"+bucket+"/"+key+request_id+".zip"
            val fileCreatedDate = new DateTime()
            val fileExpiryDate = fileCreatedDate.plusDays(days_for_expiration)
            val fileSize = new File(local_path+request_id+".zip").length()
            sc.parallelize(Array((request_id, distinctOutput.count().toInt, firstEventDate, lastEventDate, fileSize, fileCreatedDate.getMillis, fileExpiryDate.getMillis, location))).saveToCassandra("general_db", "jobs", SomeColumns("request_id", "output_events", "dt_first_event", "dt_last_event", "file_size", "dt_file_created", "dt_expiration", "location"))
        }
        sc.makeRDD(List(Empty()));
    }

    override def postProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {

        sc.makeRDD(List(Empty()));
    }
}