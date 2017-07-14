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
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.creation.model.CreationPData

object DataExhaustUtils {

    implicit val className = "org.ekstep.analytics.dataexhaust.DataExhaustUtils"

    def updateStage(request_id: String, client_key: String, satage: String, stage_status: String, status: String = "PROCESSING", err_message: String = "")(implicit sc: SparkContext) {
        sc.makeRDD(Seq(JobStage(request_id, client_key, satage, stage_status, status, err_message))).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("request_id", "client_key", "stage", "stage_status", "status", "err_message"))
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

    def saveData(rdd: RDD[String], eventConfig: EventId, requestId: String, eventId: String, outputFormat: String, requestID: String, clientKey: String)(implicit sc: SparkContext) {
        if (rdd.count() > 0) {
            val data = if (outputFormat.equalsIgnoreCase("csv")) toCSV(rdd) else rdd;
            val saveType = AppConf.getConfig("data_exhaust.save_config.save_type")
            val bucket = AppConf.getConfig("data_exhaust.save_config.bucket")
            val prefix = AppConf.getConfig("data_exhaust.save_config.prefix")
            val path = AppConf.getConfig("data_exhaust.save_config.local_path")

            saveType match {
                case "s3" =>
                    val key = "s3n://" + bucket + "/" + prefix + requestId + "/" + eventId;
                    data.saveAsTextFile(key)
                    DataExhaustUtils.updateStage(requestID, clientKey, "SAVE_DATA_TO_S3_" + eventId, "COMPLETED")
                case "local" =>
                    val localPath = prefix + requestId + "/" + eventId
                    data.saveAsTextFile(localPath)
                    DataExhaustUtils.updateStage(requestID, clientKey, "SAVE_DATA_TO_LOCAL_" + eventId, "COMPLETED")
            }
        } else {
            JobLogger.log("No data to save.", None, INFO);
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
                    if (StringUtils.equals("eks-consumption-raw", request.dataset_id.get)) {
                        val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy/MM/dd").withZoneUTC();
                        val startDate = CommonUtil.dateFormat.parseDateTime(request.filter.start_date).withTimeAtStartOfDay().getMillis
                        val endDate = CommonUtil.dateFormat.parseDateTime(request.filter.end_date).withTimeAtStartOfDay().getMillis
                        Fetcher(searchType, None, Option(Array(Query(Option(bucket), Option(prefix), Option(dateFormat.print(startDate)), Option(dateFormat.print(endDate)), None, None, None, None, None, None, Option("aggregated-"), Option("yyyy/MM/dd")))));
                    } else {
                        val queries = Array(Query(Option(bucket), Option(prefix), Option(request.filter.start_date), Option(request.filter.end_date)))
                        Fetcher(searchType, None, Option(queries))
                    }
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

    private def filterChannelAndApp(dataSetId: String, data: RDD[String], filter: Map[String, AnyRef]): RDD[String] = {
        if (List("eks-consumption-raw", "eks-creation-raw").contains(dataSetId)) {
            val filteredRDD = if ("eks-consumption-raw".equals(dataSetId)) {
                val channelFilter = (event: Event, channel: String) => {
                    if (StringUtils.isNotBlank(channel) && !AppConf.getConfig("default.channel.id").equals(channel)) {
                        channel.equals(event.channel.get);
                    } else {
                        event.channel.isEmpty || event.channel.getOrElse("").equals(AppConf.getConfig("default.channel.id"));
                    }
                };
                val appIdFilter = (event: Event, appId: String) => {
                	val defaultAppId = AppConf.getConfig("default.consumption.app.id");
                    val app = event.pdata;
                    if (StringUtils.isNotBlank(appId) && !defaultAppId.equals(appId)) {
                        appId.equals(app.getOrElse(PData("", "")).id);
                    } else {
                        app.isEmpty || defaultAppId.equals(app.getOrElse(PData("", "")).id)
                    }
                };
                val rawRDD = data.map { event =>
                    try {
                        JSONUtils.deserialize[Event](event)
                    } catch {
                        case t: Throwable =>
                            null
                    }
                }.filter { x => null != x }
                println("Input count: ", rawRDD.count())
                val channelFltrRDD = DataFilter.filter[Event, String](rawRDD, filter.getOrElse("channel", "").asInstanceOf[String], channelFilter);
                println("After channel filter count: ", channelFltrRDD.count())
                val appFltrRDD = DataFilter.filter[Event, String](channelFltrRDD, filter.getOrElse("app_id", "").asInstanceOf[String], appIdFilter);
                println("After app_id filter count: ", appFltrRDD.count())
                appFltrRDD;
            } else {
                val channelFilter = (event: CreationEvent, channel: String) => {
                    if (StringUtils.isNotBlank(channel) && !AppConf.getConfig("default.channel.id").equals(channel)) {
                        channel.equals(event.channel.get);
                    } else {
                        event.channel.isEmpty || event.channel.getOrElse("").equals(AppConf.getConfig("default.channel.id"));
                    }
                }
                val appIdFilter = (event: CreationEvent, appId: String) => {
                	val defaultAppId = AppConf.getConfig("default.creation.app.id");
                    val app = event.pdata;
                    if (StringUtils.isNotBlank(appId) && !defaultAppId.equals(appId)) {
                        appId.equals(app.getOrElse(new CreationPData("", "")).id);
                    } else {
                        true;
                    }
                }

                val rawRDD = data.map { event =>
                    try {
                        JSONUtils.deserialize[CreationEvent](event)
                    } catch {
                        case t: Throwable =>
                            null
                    }
                }.filter { x => null != x }
                println("Input count: ", rawRDD.count())
                val channelFltrRDD = DataFilter.filter[CreationEvent, String](rawRDD, filter.getOrElse("channel", "").asInstanceOf[String], channelFilter);
                println("After channel filter count: ", channelFltrRDD.count())
                val appFltrRDD = DataFilter.filter[CreationEvent, String](channelFltrRDD, filter.getOrElse("app_id", "").asInstanceOf[String], appIdFilter);
                println("After app_id filter count: ", appFltrRDD.count())
                appFltrRDD;
            }
            filteredRDD.map { x => JSONUtils.serialize(x) };
        } else {
            data;
        }
    }

    def filterEvent(data: RDD[String], filter: Map[String, AnyRef], eventId: String, dataSetId: String)(implicit exhaustConfig: Map[String, DataSet]) = {

        val rawDatasets = List("eks-consumption-raw", "eks-creation-raw");
        val orgFilterKeys = List("channel", "app_id");
        val eventConf = exhaustConfig.get(dataSetId).get.eventConfig.get(eventId).get
        val filterMapping = eventConf.filterMapping

        val filteredRDD = filterChannelAndApp(dataSetId, data, filter);

        val filterKeys = filterMapping.keySet

        val filters = filterKeys.map { key =>
            val defaultFilter = JSONUtils.deserialize[Filter](JSONUtils.serialize(filterMapping.get(key)));
            if (rawDatasets.contains(dataSetId) && orgFilterKeys.contains(key)) {
            	Filter(defaultFilter.name, defaultFilter.operator, None);
            } else {
            	if ("channel".equals(key)) {
            		val value = if (filter.get(key).isDefined) filter.get(key) else Option(AppConf.getConfig("default.channel.id"));
            		Filter(defaultFilter.name, defaultFilter.operator, value);
            	} else {
            		Filter(defaultFilter.name, defaultFilter.operator, filter.get(key));
            	}
            }
        }.filter(x => x.value.isDefined).toArray

        val finalRDD = filteredRDD.map { line =>
            try {
                val event = stringToObject(line, dataSetId);
                val matched = if (null != event) { DataFilter.matches(event._2, filters) } else false;
                if (matched) event else null;
            } catch {
                case ex: Exception =>
                    null;
            }
        }.filter { x => x != null }
        println("After tags filter count: ", finalRDD.count())
        finalRDD;
    }

    def stringToObject(event: String, dataSetId: String) = {
        try {
          dataSetId match {
            case "eks-consumption-raw" =>
                val e = JSONUtils.deserialize[Event](event);
                (CommonUtil.getEventSyncTS(e), e);
            case "eks-creation-raw" =>
                val e = JSONUtils.deserialize[CreationEvent](event);
                (CreationEventUtil.getEventSyncTS(e), e);
            case "eks-consumption-summary" | "eks-creation-summary" =>
                val e = JSONUtils.deserialize[DerivedEvent](event);
                (e.syncts, e);
            case "eks-consumption-metrics" | "eks-creation-metrics" =>
                val e = JSONUtils.deserialize[DerivedEvent](event);
                (CommonUtil.dayPeriodToLong(e.dimensions.period.getOrElse(0)), e);
        }
        } catch {
          case t: Throwable => 
              null
        }
    }
}