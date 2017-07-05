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
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.conf.AppConf
import scala.collection.JavaConversions._
import com.typesafe.config.Config
import org.ekstep.analytics.framework.DataSet
import org.ekstep.analytics.framework.Input
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.EventId

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
    def uploadZip(bucket: String, filePrefix: String, zipFileAbsolutePath: String, request_id: String, client_key: String)(implicit sc: SparkContext) {
        try {
            S3Util.uploadPublic(bucket, zipFileAbsolutePath, filePrefix);
            updateStage(request_id, client_key, "UPLOAD_ZIP", "COMPLETED")
        } catch {
            case t: Throwable =>
                updateStage(request_id, client_key, "UPLOAD_ZIP", "FAILED", "FAILED")
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

    def saveData(rdd: RDD[String], eventConfig: EventId, requestId: String, eventId: String, outputFormat: String, requestID: String, clientKey: String)(implicit sc: SparkContext) {

        val data = if (outputFormat.equalsIgnoreCase("csv")) toCSV(rdd) else rdd;
        val saveType = AppConf.getConfig("dataexhaust.save_config.save_type")
        val bucket = AppConf.getConfig("dataexhaust.save_config.bucket")
        val prefix = AppConf.getConfig("dataexhaust.save_config.prefix")
        val path = AppConf.getConfig("dataexhaust.save_config.local_path")

        saveType match {
            case "s3" =>
                val key = "s3n://" + bucket + "/" + prefix + requestId + "/" + eventId;
                data.saveAsTextFile(key)
                DataExhaustUtils.updateStage(requestID, clientKey, "SAVE_DATA_TO_S3_" + eventId, "COMPLETED")
            case "local" =>
                val localPath = path + "/" + requestId + "/" + eventId
                data.saveAsTextFile(localPath)
                DataExhaustUtils.updateStage(requestID, clientKey, "SAVE_DATA_TO_LOCAL_" + eventId, "COMPLETED")
        }
    }
    def fetchData(eventId: String, request: RequestConfig, requestID: String, clientKey: String)(implicit sc: SparkContext, exhaustConfig: Map[String, DataSet]): RDD[String] = {
        try {
            val dataSetID = request.dataset_id.get
            val eventConfig = exhaustConfig.get(dataSetID).get.eventConfig.get(eventId).get
            val searchType = eventConfig.searchType.toLowerCase()
            val fetcher = searchType match {
                case "s3" =>
                    val bucket = eventConfig.fetchConfig.params.get("bucket").get
                    val prefix = eventConfig.fetchConfig.params.get("prefix").get
                    val queries = Array(Query(Option(bucket), Option(prefix), Option(request.filter.start_date), Option(request.filter.end_date)))
                    Fetcher(searchType, None, Option(queries))
                case "local" =>
                    val filePath = eventConfig.fetchConfig.params.get("file").get
                    val queries = Array(Query(None, None, None, None, None, None, None, None, None, Option(filePath)))
                    Fetcher(searchType, None, Option(queries))
            }
            val data = DataFetcher.fetchBatchData[String](fetcher);
            DataExhaustUtils.updateStage(requestID, clientKey, "FETCH_DATA_" + eventId, "COMPLETED")
            data;
        } catch {
            case t: Throwable =>
                throw t;
        }
    }

    def deleteS3File(bucket: String, prefix: String, request_ids: Array[String]) {

        for (request_id <- request_ids) {
            val keys1 = S3Util.getPath(bucket, prefix + "/" + request_id)
            for (key <- keys1) {
                S3Util.deleteObject(bucket, key.replace(s"s3n://$bucket/", ""))
            }
            S3Util.deleteObject(bucket, prefix + "/" + request_id + "_$folder$");
        }
    }

    def filterEvent(data: RDD[String], filter: Map[String, AnyRef], eventId: String, dataSetId: String)(implicit exhaustConfig: Map[String, DataSet]) = {

        val eventConf = exhaustConfig.get(dataSetId).get.eventConfig.get(eventId).get
        val eventType = eventConf.eventType
        val filterMapping = eventConf.filterMapping

        val filterKeys = filterMapping.keySet
        val filters = filterKeys.map { key =>
            val defaultFilter = JSONUtils.deserialize[Filter](JSONUtils.serialize(filterMapping.get(key)))
            Filter(defaultFilter.name, defaultFilter.operator, filter.get(key))
        }.filter(x => None != x.value).toArray

        data.map { line =>
            try {
                val event = eventType match {
                    case "ConsumptionRaw" => JSONUtils.deserialize[Event](line);
                    case "Summary"        => JSONUtils.deserialize[DerivedEvent](line);
                    case "CreationRaw"    => JSONUtils.deserialize[CreationEvent](line);
                    case "Metrics"        => JSONUtils.deserialize[DerivedEvent](line);
                }
                val matched = DataFilter.matches(event, filters);
                if (matched) {
                    event;
                } else {
                    null;
                }
            } catch {
                case ex: Exception =>
                    null;
            }
        }.filter { x => x != null }
    }
}