package org.ekstep.analytics.dataexhaust

import scala.annotation.migration
import scala.reflect.runtime.universe
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.CsvColumnMapping
import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.DataSet
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.EventId
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.V3Event
import org.ekstep.analytics.framework.V3PData
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.exception.DataFilterException
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.util.S3Util
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.util.JobRequest
import org.ekstep.analytics.util.JobStage
import org.ekstep.analytics.util.RequestConfig
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.toNamedColumnRef
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.toSparkContextFunctions
import com.github.wnameless.json.flattener.FlattenMode
import com.github.wnameless.json.flattener.JsonFlattener
import org.ekstep.ep.samza.converter.converters.TelemetryV3Converter
import scala.collection.JavaConverters._
import com.google.gson.reflect.TypeToken
import com.google.gson.Gson
import java.lang.reflect.Type;

object DataExhaustUtils {

    implicit val className = "org.ekstep.analytics.dataexhaust.DataExhaustUtils"

    val CONSUMPTION_ENV = List("Genie", "ContentPlayer")

    def updateStage(request_id: String, client_key: String, satage: String, stage_status: String, status: String = "PROCESSING", err_message: String = "")(implicit sc: SparkContext) {
        sc.makeRDD(Seq(JobStage(request_id, client_key, satage, stage_status, status, err_message))).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("request_id", "client_key", "stage", "stage_status", "status", "err_message"))
    }

    def toCSV(rdd: RDD[String], eventConfig: EventId)(implicit sc: SparkContext): RDD[String] = {
        val data = rdd.map { x => new JsonFlattener(x).withFlattenMode(FlattenMode.KEEP_ARRAYS).flatten() }
        val dataMapRDD = data.map { x => JSONUtils.deserialize[Map[String, AnyRef]](x) }
        val allHeaders = dataMapRDD.map(f => f.keys).flatMap { x => x }.distinct().collect();
        val visibleHeaders = allHeaders.filter { h =>
            val columnMapping = eventConfig.csvConfig.columnMappings.getOrElse(h, CsvColumnMapping(to = h, hidden = false, mapFunc = null))
            !columnMapping.hidden
        }

        // removing hidden columns
        val filteredMapRdd = dataMapRDD.map { x =>
            var newData = x
            for (f <- allHeaders) yield {
                val columnMapping = eventConfig.csvConfig.columnMappings.getOrElse(f, CsvColumnMapping(to = f, hidden = false, mapFunc = null))
                if (columnMapping.hidden) {
                    newData = newData - f
                }
            }
            newData
        }

        val rows = filteredMapRdd.map { x =>
            for (f <- visibleHeaders) yield {
                val value = x.getOrElse(f, "")
                val columnMapping = eventConfig.csvConfig.columnMappings.getOrElse(f, CsvColumnMapping(to = f, hidden = false, mapFunc = null))
                if (value.isInstanceOf[List[AnyRef]]) {
                    StringEscapeUtils.escapeCsv(JSONUtils.serialize(value));
                } else {
                    val mapFuncName = columnMapping.mapFunc
                    if (mapFuncName != null) {
                        val transformed = ColumnValueMapper.mapValue(mapFuncName, value.toString())
                        StringEscapeUtils.escapeCsv(transformed);
                    } else {
                        StringEscapeUtils.escapeCsv(JSONUtils.serialize(value));
                    }
                }
            }
        }.map { x => x.mkString(",") }.collect();

        val renamedHeaders = visibleHeaders.map { header =>
            val columnMapping = eventConfig.csvConfig.columnMappings.getOrElse(header, CsvColumnMapping(to = header, hidden = false, mapFunc = null))
            if (columnMapping.to == null) header else columnMapping.to
        }

        val csv = Array(renamedHeaders.mkString(",")) ++ rows;
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
            val data = if (outputFormat.equalsIgnoreCase("csv")) toCSV(rdd, eventConfig) else rdd;
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

        println("deleteS3File----")
        for (request_id <- request_ids) {
            println("deleteing for request id: "+request_id)
            val keys1 = S3Util.getPath(bucket, prefix + "/" + request_id)
            for (key <- keys1) {
                S3Util.deleteObject(bucket, key.replace(s"s3n://$bucket/", ""))
                println("deleted: "+ key)
            }
            S3Util.deleteObject(bucket, prefix + "/" + request_id + "_$folder$");
        }
    }

    private def filterChannelAndApp(dataSetId: String, data: RDD[String], filter: Map[String, AnyRef]): RDD[String] = {
        if (List("eks-consumption-raw", "eks-creation-raw").contains(dataSetId)) {
            val convertedData = DataExhaustUtils.convertData(data)
            val filteredRDD = if ("eks-consumption-raw".equals(dataSetId)) {
                val channelFilter = (event: V3Event, channel: String) => {
                    if (StringUtils.isNotBlank(channel) && !AppConf.getConfig("default.channel.id").equals(channel)) {
                        channel.equals(event.context.channel);
                    } else {
                        event.context.channel.isEmpty || event.context.channel.equals(AppConf.getConfig("default.channel.id"));
                    }
                };
                val appIdFilter = (event: V3Event, appId: String) => {
                    val defaultAppId = AppConf.getConfig("default.consumption.app.id");
                    val app = event.context.pdata;
                    if (StringUtils.isNotBlank(appId) && !defaultAppId.equals(appId)) {
                        appId.equals(app.getOrElse(V3PData("")).id);
                    } else {
                        //app.isEmpty || null == app.get.id || defaultAppId.equals(app.getOrElse(V3PData("")).id);
                        // filter all events if the `app_id` is not mentioned
                        true;
                    }
                };
                val rawRDD = convertedData.map { event =>
                    try {
                        JSONUtils.deserialize[V3Event](event)
                    } catch {
                        case t: Throwable =>
                            null
                    }
                }.filter { x => null != x && CONSUMPTION_ENV.contains(x.context.env) }
                println("Input count: ", rawRDD.count())
                val channelFltrRDD = DataFilter.filter[V3Event, String](rawRDD, filter.getOrElse("channel", "").asInstanceOf[String], channelFilter);
                println("After channel filter count: ", channelFltrRDD.count())
                val appFltrRDD = DataFilter.filter[V3Event, String](channelFltrRDD, filter.getOrElse("app_id", "").asInstanceOf[String], appIdFilter);
                println("After app_id filter count: ", appFltrRDD.count())
                appFltrRDD;
            } else {
                val channelFilter = (event: V3Event, channel: String) => {
                    if (StringUtils.isNotBlank(channel) && !AppConf.getConfig("default.channel.id").equals(channel)) {
                        channel.equals(event.context.channel);
                    } else {
                        event.context.channel.equals(AppConf.getConfig("default.channel.id"));
                    }
                }
                val appIdFilter = (event: V3Event, appId: String) => {
                    val defaultAppId = AppConf.getConfig("default.creation.app.id");
                    val app = event.context.pdata;
                    if (StringUtils.isNotBlank(appId) && !defaultAppId.equals(appId)) {
                        appId.equals(app.getOrElse(V3PData("")).id);
                    } else {
                        true;
                    }
                }

                val rawRDD = convertedData.map { event =>
                    try {
                        JSONUtils.deserialize[V3Event](event)
                    } catch {
                        case t: Throwable =>
                            null
                    }
                }.filter { x => null != x && !CONSUMPTION_ENV.contains(x.context.env) }
                println("Input count: ", rawRDD.count())
                val channelFltrRDD = DataFilter.filter[V3Event, String](rawRDD, filter.getOrElse("channel", "").asInstanceOf[String], channelFilter);
                println("After channel filter count: ", channelFltrRDD.count())
                val appFltrRDD = DataFilter.filter[V3Event, String](channelFltrRDD, filter.getOrElse("app_id", "").asInstanceOf[String], appIdFilter);
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
                case "eks-consumption-raw" | "eks-creation-raw" =>
                    val e = JSONUtils.deserialize[V3Event](event);
                    (CommonUtil.getEventSyncTS(e), e);
                case "eks-consumption-summary" | "eks-creation-summary" | "eks-consumption-metrics" | "eks-creation-metrics" =>
                    val e = JSONUtils.deserialize[DerivedEvent](event);
                    (e.syncts, e);
            }
        } catch {
            case t: Throwable =>
                null
        }
    }

    def convertData(data: RDD[String]): RDD[String] = {
        val mapType: java.lang.reflect.Type = new TypeToken[java.util.Map[String, Object]]() {}.getType();
        data.map { x =>
            val eventMap: java.util.Map[String, Object] = new Gson().fromJson(x, mapType);
            val version = eventMap.get("ver").asInstanceOf[String]

            if (StringUtils.equals("3.0", version)) {
                Array(x);
            } else {
                try {
                    new TelemetryV3Converter(eventMap).convert().map { x => x.toJson() };
                } catch {
                    case t: Throwable =>
                        println(t.getMessage()) // TODO: handle error
                        Array("");
                }
            }
        }.flatMap { x => x }.filter { x => (x != null && x.nonEmpty) }
    }

}