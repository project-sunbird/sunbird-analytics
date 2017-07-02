package org.ekstep.analytics.job

import java.io.File
import java.io.FileWriter
import java.io.FilenameFilter
import java.io.PrintWriter
import java.util.Date

import scala.reflect.runtime.universe

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.Fetcher
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
import kafka.utils.Json

case class FileInfo(event_id: String, event_count: Long, first_event_date: String, last_event_date: String, file_size: Double)
case class ManifestFile(id: String, ver: String, ts: String, dataset_id: String, total_event_count: Long, start_date: String, end_end: String, file_info: Array[FileInfo], request: Map[String, AnyRef])

object DataExhaustPackager extends optional.Application with IJob {

    val className = "org.ekstep.analytics.model.DataExhaustPackager"
    def name: String = "DataExhaustPackager"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {

        JobLogger.init("DataExhaust Packager")

        val jobConfig = JSONUtils.deserialize[JobConfig](config);
        val fetcher = jobConfig.search

        if (null == sc.getOrElse(null)) {
            JobContext.parallelization = 10;
            implicit val sparkContext = CommonUtil.getSparkContext(JobContext.parallelization, "DataExhaust");
            try {
                execute(fetcher);
            } finally {
                CommonUtil.closeSparkContext();
            }
        } else {
            implicit val sparkContext: SparkContext = sc.getOrElse(null);
            execute(fetcher);
        }
    }

    def execute(config: Fetcher)(implicit sc: SparkContext) {
        // Get all job request with status equals PENDING_PACKAGING
        val jobRequests = sc.cassandraTable[JobRequest](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST).where("status = ?", "PENDING_PACKAGING");

        jobRequests.collect.map { request =>
            packageExhaustData(request, config)
        }
    }

    def getData(dir: File, jobRequest: JobRequest, path: String)(implicit sc: SparkContext): Array[String] = {
        CommonUtil.deleteFile(path + "/" + dir.getName + "/_SUCCESS")
        val files = dir.list(new FilenameFilter() {
            override def accept(directory: File, fileName: String): Boolean = {
                !fileName.endsWith(".crc")
            }
        });
        files.map { x =>
            sc.textFile(path + "/" + dir.getName + "/" + x)
        }.flatMap { x => x.collect() }
    }

    def packageExhaustData(jobRequest: JobRequest, config: Fetcher)(implicit sc: SparkContext) {

        val searchType = config.`type`.toString()
        val path = searchType match {
            case "s3" =>
                val bucket = config.query.get.bucket.toString() 
                val prefix = config.query.get.prefix.toString() 
                S3Util.download(bucket, prefix + jobRequest.request_id, "/tmp" + "/")
                "/tmp/" + jobRequest.request_id + "/"
            case "local" =>
                CommonUtil.deleteFile("src/test/resources/data-exhaust-package/" + jobRequest.request_id + "/.DS_Store")
                "src/test/resources/data-exhaust-package/" + jobRequest.request_id
        }

        val fileObj = new File(path.toString())
        val data = fileObj.listFiles.map { dir =>
            (dir.getName, getData(dir, jobRequest, path))
        }

        generateManifestFile(data, jobRequest)
        generateDataFiles(data, jobRequest)
        
        // TODO: Need to change the zipFolder logic in CommonUtil.
        // CommonUtil.zipFolder("/tmp/target" + ".zip", "/tmp/target/data")
        
        //TODO: Needs to write code to upload zip
        
    }

    def generateManifestFile(data: Array[(String, Array[String])], jobRequest: JobRequest) {
        val fileInfo = data.map { f =>
            val firstEvent = JSONUtils.deserialize[Map[String, AnyRef]](f._2.head)
            val lastEvent = JSONUtils.deserialize[Map[String, AnyRef]](f._2.last)
            FileInfo(f._1, f._2.length, new Date(firstEvent.get("ets").get.asInstanceOf[Number].longValue()).toLocaleString(), new Date(lastEvent.get("ets").get.asInstanceOf[Number].longValue()).toLocaleString(), 0.0)
        }
        val totalEventCount = data.map { f => f._2.length }.sum
        val requestData = JSONUtils.deserialize[Map[String, AnyRef]](jobRequest.request_data)
        val filter = requestData.get("filter").get.asInstanceOf[Map[String, AnyRef]]
        val manifest = ManifestFile("ekstep.analytics.dataset", "1.0", new Date(System.currentTimeMillis()).toLocaleString(), requestData.get("dataset_id").get.toString(), totalEventCount, filter.get("startDate").toString(), filter.get("endDate").toString(), fileInfo, requestData)
        val gson = new GsonBuilder().setPrettyPrinting().create();
        val jsonString = gson.toJson(manifest)
        writeToFile(Array(jsonString), "/tmp/target/" + jobRequest.request_id + "/", "manifest.json")
    }

    def generateDataFiles(data: Array[(String, Array[String])], jobRequest: JobRequest) {
        val fileExtenstion = JSONUtils.deserialize[Map[String, AnyRef]](jobRequest.request_data).get("output_format").get.toString().toLowerCase()
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