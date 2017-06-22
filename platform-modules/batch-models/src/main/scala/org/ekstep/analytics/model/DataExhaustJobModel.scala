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
import org.ekstep.analytics.util._
import org.joda.time.DateTime
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.toNamedColumnRef
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.toSparkContextFunctions
import com.github.wnameless.json.flattener.FlattenMode
import com.github.wnameless.json.flattener.JsonFlattener
import org.ekstep.analytics.framework.Event
import org.apache.commons.lang3.StringEscapeUtils
import org.ekstep.analytics.dataexhaust.DataExhaustUtils
import org.ekstep.analytics.util._

object DataExhaustJobModel extends IBatchModel[String, JobResponse] with Serializable {

    val className = "org.ekstep.analytics.model.DataExhaustJobModel"
    override def name: String = "DataExhaustJobModel"

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
        val dataSetId = config.getOrElse("dataset_id", "D002").asInstanceOf[String]
        val requestFilter = DataExhaustUtils.getRequest(request_id, client_key)
        try {
            val filteredData = DataExhaustUtils.filterEvent(data, requestFilter, dataSetId);
            DataExhaustUtils.updateStage(request_id, client_key, "FILTERING_DATA", "COMPLETED")
            filteredData
        } catch {
            case t: Throwable =>
                DataExhaustUtils.updateStage(request_id, client_key, "FILTERING_DATA", "FAILED", "FAILED")
                throw t;
        }
    }

    def algorithm(data: RDD[DataExhaustJobInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[JobResponse] = {

        val request_id = config.get("request_id").get.asInstanceOf[String];
        val bucket = config.get("data-exhaust-bucket").get.asInstanceOf[String]
        val prefix = config.get("data-exhaust-prefix").get.asInstanceOf[String]
        val dispatch_to = config.getOrElse("dispatch-to", "local").asInstanceOf[String];

        val events = data.cache
        val outputFormat = config.get("output_format").get.asInstanceOf[String]
        val res = dispatch_to match {
            case "local" =>
                val localPath = config.get("path").get.asInstanceOf[String] + "/" + request_id;
                DataExhaustUtils.saveData(outputFormat, localPath, events, localPath, "SAVE_DATA_TO_LOCAL", config)
            case "s3" =>
                val uploadPrefix = prefix + "/" + request_id;
                val key = "s3n://" + bucket + "/" + uploadPrefix;
                DataExhaustUtils.saveData(outputFormat, key, events, uploadPrefix, "SAVE_DATA_TO_S3", config);
        }
        events.unpersist(true)
        res;
    }

    def postProcess(data: RDD[JobResponse], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[JobResponse] = {
        data;
    }

    def main(args: Array[String]): Unit = {
        val requestId = "6a54bfa283de43a89086"
        val localPath = "/tmp/dataexhaust/6a54bfa283de43a89086"
        CommonUtil.deleteFile(localPath + "/" + requestId + "_$folder$")
        CommonUtil.deleteFile(localPath + "/_SUCCESS")
        CommonUtil.zipFolder(localPath + ".zip", localPath)
    }
}