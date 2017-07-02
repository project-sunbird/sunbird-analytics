package org.ekstep.analytics.job

import java.io.File
import java.io.FileWriter
import java.io.PrintWriter
import java.util.Date
import scala.reflect.runtime.universe
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.DataSet
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.util.S3Util
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.util.JobRequest
import com.datastax.spark.connector.toSparkContextFunctions
import com.google.gson.GsonBuilder

case class FileInfo(event_id: String, event_count: Long, first_event_date: String, last_event_date: String, file_size: Double)
case class ManifestFile(id: String, ver: String, ts: String, dataset_id: String, total_event_count: Long, start_date: String, end_end: String, file_info: Array[FileInfo], request: Map[String, AnyRef])

object DataExhaustPackager extends optional.Application with IJob {

    val className = "org.ekstep.analytics.model.DataExhaustPackager"
    def name: String = "DataExhaustPackager"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {

        JobLogger.init("DataExhaust Packager")

        val jobConfig = JSONUtils.deserialize[Map[String, AnyRef]](config);

        if (null == sc.getOrElse(null)) {
            JobContext.parallelization = 10;
            implicit val sparkContext = CommonUtil.getSparkContext(JobContext.parallelization, "DataExhaust");
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

    def execute(config: Map[String, AnyRef])(implicit sc: SparkContext) {
        // Get all job request with status equals PENDING_PACKAGING
        val jobRequests = sc.cassandraTable[JobRequest](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST).where("status = ?", "PENDING_PACKAGING");

        jobRequests.map { request =>
            packageExhaustData(request, config)
        }
    }

    def packageExhaustData(jobRequest: JobRequest, config: Map[String, AnyRef])(implicit sc: SparkContext) {

        val searchType = config.get("searchType").get.toString()
        val path = searchType match {
            case "s3" =>
                val bucket = config.get("bucket").get.toString()
                val prefix = config.get("prefix").get.toString()
                S3Util.download(bucket, prefix + jobRequest.request_id, "/tmp" + "/")
                "/tmp/" + jobRequest.request_id + "/"
            case "local" =>
                "src/test/resources/data-exhaust-package"
        }

        // Read all data from local file system
        val fileObj = new File(path)

        val data = fileObj.listFiles.map { dir =>
            (dir.getName, getData(dir, jobRequest))
        }
        generateManifestFile(data, jobRequest)
        generateDataFiles(data, jobRequest)
        // TODO: Need to change the zipFolder logic in CommonUtil
        // CommonUtil.zipFolder("/tmp/target" + ".zip", "/tmp/target/data")
    }

    def getData(dir: File, jobRequest: JobRequest)(implicit sc: SparkContext): Array[String] = {
        dir.listFiles.map { file =>
            CommonUtil.deleteFile("/tmp" + jobRequest.request_id + "_$folder$")
            CommonUtil.deleteFile("/tmp" + "/_SUCCESS")
            sc.textFile(file.getAbsolutePath)
        }.flatMap { x => x.collect() }
    }

    def generateManifestFile(data: Array[(String, Array[String])], jobRequest: JobRequest) {
        val fileInfo = data.map { f =>
            val firstEvent = JSONUtils.deserialize[Map[String, AnyRef]](f._2.head)
            val lastEvent = JSONUtils.deserialize[Map[String, AnyRef]](f._2.last)
            FileInfo(f._1, f._2.length, new Date(firstEvent.get("ets").get.asInstanceOf[Number].longValue()).toLocaleString(), new Date(lastEvent.get("ets").get.asInstanceOf[Number].longValue()).toLocaleString(), 0.0)
        }
        val totalEventCount = data.map { f => f._2.length }.sum
        val requestData = jobRequest.request_data.asInstanceOf[Map[String, AnyRef]]
        val manifest = ManifestFile("ekstep.analytics.dataset", "1.0", new Date(System.currentTimeMillis()).toLocaleString(), requestData.get("dataset_id").toString(), totalEventCount, requestData.get("startDate").toString(), requestData.get("endDate").toString(), fileInfo, requestData)

        val gson = new GsonBuilder().setPrettyPrinting().create();
        val jsonString = gson.toJson(manifest)
        writeToFile(Array(jsonString), "/tmp/target" + jobRequest.request_id + "/", "manifest.json")
    }

    def generateDataFiles(data: Array[(String, Array[String])], jobRequest: JobRequest) {
        val fileExtenstion = jobRequest.request_data.asInstanceOf[Map[String, AnyRef]].get("output_format").get.toString().toLowerCase()
        data.foreach { events =>
            val fileName = events._1
            val fileData = events._2
            writeToFile(fileData, "/tmp/target/" + jobRequest.request_id + "/data/", fileName + "." + fileExtenstion)
        }
    }

    def writeToFile(data: Array[String], path: String, fileName: String) {
        if (!new java.io.File(path).exists()) {
            new File(path).mkdirs()
        }
        val out = new PrintWriter(new FileWriter(path + fileName))
        try {
            data.foreach { x =>
                out.write(x)
                out.write("\n")
            }
        } finally {
            out.close
        }
    }
}