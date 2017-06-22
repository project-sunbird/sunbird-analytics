package org.ekstep.analytics.dataexhaust

import scala.reflect.runtime.universe
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.S3Util
import org.ekstep.analytics.util._
import org.ekstep.analytics.util.Constants
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.toNamedColumnRef
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.toSparkContextFunctions
import com.github.wnameless.json.flattener.FlattenMode
import com.github.wnameless.json.flattener.JsonFlattener
import org.ekstep.analytics.creation.model.CreationEvent
import org.ekstep.analytics.framework.MeasuredEvent

object DataExhaustUtils {

    def updateStage(request_id: String, client_key: String, satage: String, stage_status: String, status: String = "PROCESSING")(implicit sc: SparkContext) {
        sc.makeRDD(Seq(JobStage(request_id, client_key, satage, stage_status, status))).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("request_id", "client_key", "stage", "stage_status", "status"))
    }
    def toCSV(rdd: RDD[String])(implicit sc: SparkContext): RDD[String] = {
        val data = rdd.map { x => new JsonFlattener(x).withFlattenMode(FlattenMode.KEEP_ARRAYS).flatten() }
        val dataMapRDD = data.map { x => JSONUtils.deserialize[Map[String, AnyRef]](x) }
        val headers = dataMapRDD.map(f => f.keys).flatMap { x => x }.distinct().collect();
        val rows = dataMapRDD.map { x =>
            for (f <- headers) yield {
                val value = x.getOrElse(f, "")
                if (value.isInstanceOf[List[AnyRef]]) {
                    StringEscapeUtils.escapeCsv(JSONUtils.serialize(value));
                } else if (value.isInstanceOf[Long]) {
                    StringEscapeUtils.escapeCsv(value.asInstanceOf[Long].toString());
                } else if (value.isInstanceOf[Double]) {
                    StringEscapeUtils.escapeCsv(value.asInstanceOf[Double].toString());
                } else if (value.isInstanceOf[Integer]) {
                    StringEscapeUtils.escapeCsv(value.asInstanceOf[Integer].toString());
                } else {
                    StringEscapeUtils.escapeCsv(value.asInstanceOf[String]);
                }

            }
        }.map { x => x.mkString(",") }.collect();

        val csv = Array(headers.mkString(",")) ++ rows;
        sc.parallelize(csv, 1);
    }
    def saveData(outputFormat: String, path: String, events: RDD[DataExhaustJobInput], uploadPrefix: String, stage: String, config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[JobResponse] = {

        val client_key = config.get("client_key").get.asInstanceOf[String];
        val request_id = config.get("request_id").get.asInstanceOf[String];
        val bucket = config.get("data-exhaust-bucket").get.asInstanceOf[String]
        val job_id = config.get("job_id").get.asInstanceOf[String];
        try {
            val output_events = events.count
            if (output_events > 0) {
                val firstEventDate = events.sortBy { x => x.eventDate }.first().eventDate;
                val lastEventDate = events.sortBy({ x => x.eventDate }, false).first.eventDate;
                val rawEventsRDD = events.map { x => x.event };
                //  type check for file type: json or csv
                val outputRDD = if (outputFormat.equalsIgnoreCase("csv")) toCSV(rawEventsRDD) else rawEventsRDD;
                outputRDD.saveAsTextFile(path);
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
    def filterEvent(data: RDD[String], requestFilter: RequestFilter, datasetId: String): RDD[DataExhaustJobInput] = {

        val startDate = CommonUtil.dateFormat.parseDateTime(requestFilter.start_date).withTimeAtStartOfDay().getMillis;
        val endDate = CommonUtil.dateFormat.parseDateTime(requestFilter.end_date).withTimeAtStartOfDay().getMillis + 86399000;
        val filters: Array[Filter] = Array(
            Filter("eventts", "RANGE", Option(Map("start" -> startDate, "end" -> endDate)))) ++ {
                if (requestFilter.events.isDefined && requestFilter.events.get.nonEmpty) Array(Filter("eid", "IN", Option(requestFilter.events.get))) else Array[Filter]();
            } ++ {
                if (requestFilter.tags.isDefined && requestFilter.tags.get.nonEmpty) Array(Filter("genieTag", "IN", Option(requestFilter.tags.get))) else Array[Filter]();
            }
        data.map { x =>
            try {
                val eventWithTS = datasetId match {
                    case "D002" =>
                        val e = JSONUtils.deserialize[Event](x);
                        (e, CommonUtil.getEventTS(e))
                    case "D005" =>
                        val me = JSONUtils.deserialize[CreationEvent](x);
                        (me, CommonUtil.getTimestamp(me.`@timestamp`))
                    case _ =>
                        val ce = JSONUtils.deserialize[MeasuredEvent](x);
                        (ce, ce.syncts)
                }
                val matched = DataFilter.matches(eventWithTS._1, filters);
                if (matched) {
                    DataExhaustJobInput(eventWithTS._2, x)
                } else {
                    null;
                }
            } catch {
                case ex: Exception =>
                    null;
            }
        }.filter { x => x != null }
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
    def getAllRequest()(implicit sc: SparkContext): RDD[JobRequest] = {
        try {
            val jobReq = sc.cassandraTable[JobRequest](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST).filter { x => x.status.equals("SUBMITTED") }.cache;
            if (!jobReq.isEmpty()) {
                jobReq.map { x => JobStage(x.request_id, x.client_key, "FETCHING_ALL_REQUEST", "COMPLETED", "PROCESSING") }.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("request_id", "client_key", "stage", "stage_status", "status"))
                jobReq;
            } else {
                null;
            }

        } catch {
            case t: Throwable => null;
        }
    }
    def uploadZip(response: JobResponse, localPath: String)(implicit sc: SparkContext) {
        try {
            S3Util.uploadPublic(response.bucket, localPath + ".zip", response.prefix + ".zip");
            S3Util.deleteObject(response.bucket, response.prefix);
            S3Util.deleteObject(response.bucket, response.prefix + "_$folder$");
            CommonUtil.deleteFile(localPath + ".zip")
            updateStage(response.request_id, response.client_key, "UPLOAD_ZIP", "COMPLETED")
        } catch {
            case t: Throwable =>
                updateStage(response.request_id, response.client_key, "UPLOAD_ZIP", "FAILED", "FAILED")
                throw t
        }
    }
    def downloadOutput(response: JobResponse, localPath: String)(implicit sc: SparkContext) {
        try {
            S3Util.download(response.bucket, response.prefix, localPath + "/")
            CommonUtil.deleteFile(localPath + "/" + response.request_id + "_$folder$")
            CommonUtil.deleteFile(localPath + "/_SUCCESS")
            CommonUtil.zipFolder(localPath + ".zip", localPath)
            CommonUtil.deleteDirectory(localPath);
            updateStage(response.request_id, response.client_key, "DOWNLOAD_AND_ZIP_OUTPUT_FILE", "COMPLETED")
        } catch {
            case t: Throwable =>
                updateStage(response.request_id, response.client_key, "DOWNLOAD_AND_ZIP_OUTPUT_FILE", "FAILED", "FAILED")
                throw t
        }
    }
    def fetchData(request: JobRequest, config: JobConfig)(implicit sc: SparkContext): RDD[String] = {
        try {
            val requestData = JSONUtils.deserialize[RequestConfig](request.request_data);
            val dataSetID = requestData.dataset_id.get
            val events = requestData.filter.events.getOrElse(List()).toArray
            val filter = requestData.filter

            val modelParams = config.modelParams.get;
            val fetcher = getSearchQuery(dataSetID, events, config, filter, modelParams)
            val data = DataFetcher.fetchBatchData[String](fetcher);
            updateStage(request.request_id, request.client_key, "FETCHING_DATA", "COMPLETED", "PROCESSING")
            data;
        } catch {
            case t: Throwable =>
                updateStage(request.request_id, request.client_key, "FETCHING_DATA", "FAILED", "FAILED")
                throw t;
        }

    }
    def getFetcher(bucket: String, events: Array[String], filter: RequestFilter): Fetcher = {
        val prefixes = events.map { x => if (x.endsWith("METRICS")) x.toLowerCase() else getPrefixes(x) }.filter { x => !StringUtils.equals("", x) }
        val queries = prefixes.map { x => Query(Option(bucket), Option(x), Option(filter.start_date), Option(filter.end_date)) }
        if (queries.isEmpty) Fetcher("s3", None, None); else Fetcher("s3", None, Option(queries));

    }
    def getPrefixes(event: String): String = {
        event match {
            case "ME_SESSION_SUMMARY"          => "ss"
            case "ME_ITEM_SUMMARY"             => "is"
            case "ME_GENIE_LAUNCH_SUMMARY"     => "gls"
            case "ME_CONTENT_USAGE_SUMMARY"    => "cus"
            case "ME_GENIE_USAGE_SUMMARY"      => "genie-launch-summ"
            case "ME_ITEM_USAGE_SUMMARY"       => "item-usage-summ"
            case "ME_APP_SESSION_SUMMARY"      => "app-ss"
            case "ME_CE_SESSION_SUMMARY"       => "ce-ss"
            case "ME_TEXTBOOK_SESSION_SUMMARY" => "textbook-ss"
            case "ME_APP_USAGE_SUMMARY"        => "app-usage"
            case "ME_CE_USAGE_SUMMARY"         => "ce-usage"
            case "ME_TEXTBOOK_USAGE_SUMMARY"   => "textbook-usage"
            case "consumption-summary"         => "ss,is,gls,cus,genie-launch-summ,item-usage-summ"
            case "consumption-metrics"         => "me_content_usage_metrics,me_item_usage_metrics,me_genie_usage_metrics,me_content_snapshot_metrics,me_concept_snapshot_metrics,me_asset_snapshot_metrics"
            case "creation-summary"            => "app-ss,ce-ss,textbook-ss,app-usage,ce-usage,textbook-usage"
            case "creation-metrics"            => "me_app_usage_metrics,me_ce_usage_metrics,me_textbook_creation_metrics,me_textbook_snapshot_metrics"
            case _                             => ""
        }
    }
    def getSearchQuery(dataSetID: String, events: Array[String], config: JobConfig, filter: RequestFilter, modelParams: Map[String, AnyRef]): Fetcher = {

        config.search.`type`.toLowerCase() match {
            case "s3" =>
                val dataSetRawBucket = modelParams.get("dataset-raw-bucket").getOrElse("ekstep-datasets").asInstanceOf[String];
                val dataSetRawPrefix = modelParams.get("dataset-raw-prefix").getOrElse("restricted/D001/4208ab995984d222b59299e5103d350a842d8d41/").asInstanceOf[String];
                val bucket = modelParams.get("bucket").getOrElse("ekstep-dev-data-store").asInstanceOf[String];
                dataSetID match {
                    case "D002" =>
                        val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy/MM/dd").withZoneUTC();
                        val startDate = CommonUtil.dateFormat.parseDateTime(filter.start_date).withTimeAtStartOfDay().getMillis
                        val endDate = CommonUtil.dateFormat.parseDateTime(filter.end_date).withTimeAtStartOfDay().getMillis
                        Fetcher("s3", None, Option(Array(Query(Option(dataSetRawBucket), Option(dataSetRawPrefix), Option(dateFormat.print(startDate)), Option(dateFormat.print(endDate)), None, None, None, None, None, None, Option("aggregated-"), Option("yyyy/MM/dd")))));
                    case "D003" =>
                        val finalEvents = if (events.isEmpty) Array("consumption-summary") else events
                        getFetcher(bucket, finalEvents, filter)
                    case "D004" =>
                        val finalEvents = if (events.isEmpty) Array("consumption-metrics") else events
                        getFetcher(bucket, events, filter)
                    case "D005" =>
                        Fetcher("s3", None, Option(Array(Query(Option(bucket), Option(dataSetRawPrefix), Option(filter.start_date), Option(filter.end_date)))));
                    case "D006" =>
                        val finalEvents = if (events.isEmpty) Array("creation-summary") else events
                        getFetcher(bucket, events, filter)
                    case "D007" =>
                        val finalEvents = if (events.isEmpty) Array("creation-metrics") else events
                        getFetcher(bucket, events, filter)
                    case _ =>
                        null
                }
            case "local" =>
                Fetcher("local", None, config.search.queries);
        }

    }
}