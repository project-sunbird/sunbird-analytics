package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import scala.util.control.Breaks._
import org.ekstep.analytics.dataexhaust._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util._
import org.ekstep.analytics.util.JobRequest
import org.ekstep.analytics.util.RequestConfig
import scala.collection.JavaConversions._
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import org.ekstep.analytics.framework.conf.AppConf
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import java.util.Date
import org.joda.time.DateTimeZone
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants

object DataExhaustJob extends optional.Application with IJob {

    implicit val className = "org.ekstep.analytics.job.DataExhaustJob"
    def name: String = "DataExhaustJob"
    val rawDataSetList = List("eks-consumption-raw", "eks-creation-raw")

    def main(config: String)(implicit sc: Option[SparkContext] = None) {

        JobLogger.init("DataExhaustJob")
        JobLogger.start("DataExhaust Job Started executing", Option(Map("config" -> config)))
        val jobConfig = JSONUtils.deserialize[JobConfig](config);

        if (null == sc.getOrElse(null)) {
            JobContext.parallelization = 10;
            implicit val sparkContext = CommonUtil.getSparkContext(JobContext.parallelization, jobConfig.appName.getOrElse(jobConfig.model));
            try {
                execute(jobConfig);
            } finally {
                CommonUtil.closeSparkContext();
            }
        } else {
            implicit val sparkContext: SparkContext = sc.getOrElse(null);
            execute(jobConfig);
        }
    }

    private def execute(config: JobConfig)(implicit sc: SparkContext) = {

        val requests = DataExhaustUtils.getAllRequest
        if (null != requests) {
            _executeRequests(requests.collect(), config);
        } else {
            JobLogger.end("DataExhaust Job Completed. But There is no job request in DB", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> 0, "outputEvents" -> 0, "timeTaken" -> 0)));
        }
    }

    private def _executeRequests(requests: Array[JobRequest], config: JobConfig)(implicit sc: SparkContext) = {

        val time = CommonUtil.time({
            val modelParams = config.modelParams.get
            implicit val exhaustConfig = config.exhaustConfig.get
            for (request <- requests) {
                try {
                    val requestData = JSONUtils.deserialize[RequestConfig](request.request_data);
                    val requestID = request.request_id
                    val clientKey = request.client_key
                    val eventList = requestData.filter.events.getOrElse(List())
                    val dataSetId = requestData.dataset_id.get

                    val events = if (rawDataSetList.contains(dataSetId) || eventList.size == 0)
                        exhaustConfig.get(dataSetId).get.events
                    else
                        eventList

                    for (eventId <- events) {
                        _executeEventExhaust(eventId, requestData, requestID, clientKey)
                    }
                } catch {
                    case ex: Exception =>
                        DataExhaustUtils.updateStage(request.request_id, request.client_key, "", "", "FAILED")
                }
            }
            DataExhaustPackager.execute()
            requests.length
        })
        JobLogger.end("DataExhaust Job Completed. But There is no job request in DB", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> 0, "outputEvents" -> 0, "timeTaken" -> time._1, "jobCount" -> time._2)));
    }
    private def _executeEventExhaust(eventId: String, request: RequestConfig, requestID: String, clientKey: String)(implicit sc: SparkContext, exhaustConfig: Map[String, DataSet]) = {
        val dataSetID = request.dataset_id.get
        val data = DataExhaustUtils.fetchData(eventId, request, requestID, clientKey)
        val filter = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(request.filter))
        val filteredData = DataExhaustUtils.filterEvent(data, filter, eventId, dataSetID);
        DataExhaustUtils.updateStage(requestID, clientKey, "FILTERED_DATA_" + eventId, "COMPLETED")
        val eventConfig = exhaustConfig.get(dataSetID).get.eventConfig.get(eventId).get
        val outputFormat = request.output_format.getOrElse("json")

        if ("DEFAULT".equals(eventId) && request.filter.events.isDefined && request.filter.events.get.size > 0) {
            for (event <- request.filter.events.get) {
                val filterKey = Filter("eventId", "EQ", Option(event))
                val data = DataFilter.filter(filteredData, filterKey).map { x => JSONUtils.serialize(x) }
                DataExhaustUtils.saveData(data, eventConfig, requestID, event, outputFormat, requestID, clientKey)
            }
        } else {
            DataExhaustUtils.saveData(filteredData.map { x => JSONUtils.serialize(x) }, eventConfig, requestID, eventId, outputFormat, requestID, clientKey)
        }
        DataExhaustUtils.updateStage(requestID, clientKey, "SAVE_DATA_TO_S3/LOCAL", "COMPLETED", "PENDING_PACKAGING")

    }
}