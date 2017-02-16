package org.ekstep.analytics.model

import scala.reflect.runtime.universe


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.job.JobStage
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTime

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.toNamedColumnRef
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.toSparkContextFunctions
import com.github.wnameless.json.flattener.FlattenMode
import com.github.wnameless.json.flattener.JsonFlattener
import org.ekstep.analytics.framework.Event

case class JobRequest(file_type: String, client_key: String, request_id: String, job_id: Option[String], status: String, request_data: String,
                      location: Option[String], dt_file_created: Option[DateTime], dt_first_event: Option[DateTime], dt_last_event: Option[DateTime],
                      dt_expiration: Option[DateTime], iteration: Option[Int], dt_job_submitted: DateTime, dt_job_processing: Option[DateTime],
                      dt_job_completed: Option[DateTime], input_events: Option[Long], output_events: Option[Long], file_size: Option[Long], latency: Option[Int],
                      execution_time: Option[Long], err_message: Option[String], stage: Option[String], stage_status: Option[String]) extends AlgoOutput

case class RequestFilter(start_date: String, end_date: String, tags: List[String], events: Option[List[String]]);
case class RequestConfig(filter: RequestFilter);
case class RequestOutput(request_id: String, output_events: Int)
case class DataExhaustJobInput(eventDate: Long, event: String) extends AlgoInput;
case class JobResponse(client_key: String, request_id: String, job_id: String, output_events: Long, bucket: String, prefix: String, first_event_date: Long, last_event_date: Long);

object DataExhaustJobModel extends IBatchModel[String, JobResponse] with Serializable {

    val className = "org.ekstep.analytics.model.DataExhaustJobModel"
    override def name: String = "DataExhaustJobModel"

    def updateStage(request_id: String, client_key: String, satage: String, stage_status: String, status: String = "PROCESSING")(implicit sc: SparkContext) {
        sc.makeRDD(Seq(JobStage(request_id, client_key, satage, stage_status, status))).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("request_id", "client_key", "stage", "stage_status", "status"))
    }

    def getRequest(request_id: String, client_key: String)(implicit sc: SparkContext): RequestFilter = {
        try {
            val request = sc.cassandraTable[JobRequest](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST).where("client_key = ? and request_id = ?", client_key, request_id).first();
            val filter = JSONUtils.deserialize[RequestConfig](request.request_data).filter;
            updateStage(request_id, client_key, "FETCHING_THE_REQUEST", "COMPLETED")
            filter;
        } catch {
            case t: Throwable =>
                updateStage(request_id, client_key, "FETCHING_THE_REQUEST", "FAILED", "FAILED")
                throw t;
        }
    }

    def filterEvent(data: RDD[String], requestFilter: RequestFilter): RDD[DataExhaustJobInput] = {

        val startDate = CommonUtil.dateFormat.parseDateTime(requestFilter.start_date).withTimeAtStartOfDay().getMillis;
        val endDate = CommonUtil.dateFormat.parseDateTime(requestFilter.end_date).withTimeAtStartOfDay().getMillis + 86399000;
        val filters: Array[Filter] = Array(
            Filter("eventts", "RANGE", Option(Map("start" -> startDate, "end" -> endDate))),
            Filter("genieTag", "IN", Option(requestFilter.tags))) ++ {
                if (requestFilter.events.isDefined && requestFilter.events.get.nonEmpty) Array(Filter("eid", "IN", Option(requestFilter.events.get))) else Array[Filter]();
            }
        data.map { x =>
            try {
                val event = JSONUtils.deserialize[Event](x);
                val matched = DataFilter.matches(event, filters);
                if (matched) {
                    DataExhaustJobInput(CommonUtil.getEventTS(event), x)
                } else {
                    null;
                }
            } catch {
                case ex: Exception =>
                    null;
            }
        }.filter { x => x != null }
    }

    override def execute(events: RDD[String], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[JobResponse] = {

        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val inputRDD = preProcess(events, config);
        JobContext.recordRDD(inputRDD);
        val outputRDD = algorithm(inputRDD, config);
        JobContext.recordRDD(outputRDD);
        val resultRDD = postProcess(outputRDD, config);
        JobContext.recordRDD(resultRDD);
        resultRDD
    }

    def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DataExhaustJobInput] = {

        val request_id = config.get("request_id").get.asInstanceOf[String];
        val client_key = config.get("client_key").get.asInstanceOf[String];
        val requestFilter = getRequest(request_id, client_key)
        try {
            val filteredData = filterEvent(data, requestFilter);
            updateStage(request_id, client_key, "FILTERING_DATA", "COMPLETED")
            filteredData;
        } catch {
            case t: Throwable =>
                updateStage(request_id, client_key, "FILTERING_DATA", "FAILED", "FAILED")
                throw t;
        }
    }

    private def saveData(fileType: String, path: String, events: RDD[DataExhaustJobInput], uploadPrefix: String, stage: String, config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[JobResponse] = {

        val client_key = config.get("client_key").get.asInstanceOf[String];
        val request_id = config.get("request_id").get.asInstanceOf[String];
        val bucket = config.get("data-exhaust-bucket").get.asInstanceOf[String]
        val job_id = config.get("job_id").get.asInstanceOf[String];
        try {
            val output_events = events.count
            if (output_events > 0) {
                val firstEventDate = events.sortBy { x => x.eventDate }.first().eventDate;
                val lastEventDate = events.sortBy({ x => x.eventDate }, false).first.eventDate;
                //  type check for file type: json or csv
                if(fileType.toLowerCase().equals("json")) {
                  events.map { x => x.event }.saveAsTextFile(path); 
                } else {
                  val rdd = events.map { x => JSONUtils.deserialize[Event](x.event)}
                  toCSV(rdd).coalesce(2).saveAsTextFile(path)
                }
                updateStage(request_id, client_key, stage, "COMPLETED")
                sc.makeRDD(List(JobResponse(client_key, request_id, job_id, output_events, bucket, uploadPrefix, firstEventDate, lastEventDate)));
            } else {
                updateStage(request_id, client_key, stage, "COMPLETED")
                sc.makeRDD(List(JobResponse(client_key, request_id, job_id, 0, bucket, null, 0L, 0L)));
            }
        } catch {
            case t: Throwable =>
                updateStage(request_id, client_key, stage, "FAILED", "FAILED")
                throw t;
        }

    }
    def algorithm(data: RDD[DataExhaustJobInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[JobResponse] = {

        val request_id = config.get("request_id").get.asInstanceOf[String];
        val bucket = config.get("data-exhaust-bucket").get.asInstanceOf[String]
        val prefix = config.get("data-exhaust-prefix").get.asInstanceOf[String]
        val dispatch_to = config.getOrElse("dispatch-to", "local").asInstanceOf[String];

        val events = data.cache
        val res = dispatch_to match {
            case "local" =>
                val localPath = config.get("path").get.asInstanceOf[String] + "/" + request_id;
                saveData(config.get("fileType").get.asInstanceOf[String], localPath, events, localPath, "SAVE_DATA_TO_LOCAL", config)
            case "s3" =>
                val uploadPrefix = prefix + "/" + request_id;
                val key = "s3n://" + bucket + "/" + uploadPrefix;
                saveData(config.get("fileType").get.asInstanceOf[String], key, events, uploadPrefix, "SAVE_DATA_TO_S3", config);
        }
        events.unpersist(true)
        res;
    }

    def postProcess(data: RDD[JobResponse], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[JobResponse] = {

        data;
    }
    
    def toCSV(rdd: RDD[Event])(implicit sc: SparkContext): RDD[String] = {
        val data = rdd.map { x => JSONUtils.serialize(x) }.map { x => new JsonFlattener(x).withFlattenMode(FlattenMode.KEEP_ARRAYS).flatten() }
        val dataMapRDD = data.map { x => JSONUtils.deserialize[Map[String, AnyRef]](x) }
        val header = sc.parallelize(Seq(dataMapRDD.first().keys.mkString(",")))
        val rows = dataMapRDD.map(f => f.values.map { x => if(x.isInstanceOf[List[Any]]) JSONUtils.serialize(JSONUtils.serialize(x)) else x }.mkString(","));
        header.union(rows)
    }
    def main(args: Array[String]): Unit = {
        val requestId = "6a54bfa283de43a89086"
        val localPath = "/tmp/dataexhaust/6a54bfa283de43a89086"
        CommonUtil.deleteFile(localPath + "/" + requestId + "_$folder$")
        CommonUtil.deleteFile(localPath + "/_SUCCESS")
        CommonUtil.zipFolder(localPath + ".zip", localPath)
    }
}