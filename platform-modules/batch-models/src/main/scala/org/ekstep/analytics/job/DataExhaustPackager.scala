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
import org.ekstep.analytics.dataexhaust.DataExhaustUtils
import org.ekstep.analytics.framework.EventId
import java.util.UUID
import org.ekstep.analytics.util.RequestConfig
import org.joda.time.DateTime

case class PackagerConfig(saveType: String, bucket: String, prefix: String, public_S3URL: String, localPath: String)
case class FileInfo(event_id: String, event_count: Long, first_event_date: String, last_event_date: String, file_size: Double)
case class ManifestFile(id: String, ver: String, ts: String, dataset_id: String, total_event_count: Long, start_date: String, end_end: String, file_info: Array[FileInfo], request: Map[String, AnyRef])
case class Response(request_id: String, client_key: String, job_id: String, metadata: ManifestFile, location: String, stats: Map[String, Any], jobRequest: JobRequest)

object DataExhaustPackager extends optional.Application with IJob {

    val className = "org.ekstep.analytics.model.DataExhaustPackager"
    def name: String = "DataExhaustPackager"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {

        JobLogger.init("DataExhaust Packager")

        val jobConfig = JSONUtils.deserialize[JobConfig](config);
        val eventConf = jobConfig.exhaustConfig.get("eks-consumption-raw").eventConfig.get("DEFAULT").get

        val bucket = eventConf.saveConfig.params.getOrElse("bucket", "ekstep-public-dev")
        val prefix = eventConf.saveConfig.params.getOrElse("prefix", "data-exhaust/test/")
        val localPath = eventConf.localPath
        val publicS3URL = jobConfig.modelParams.get.getOrElse("public_S3URL", "https://s3-ap-southeast-1.amazonaws.com").asInstanceOf[String]
        val conf = PackagerConfig(eventConf.saveType, bucket, prefix, publicS3URL, localPath)

        if (null == sc.getOrElse(null)) {
            JobContext.parallelization = 10;
            implicit val sparkContext = CommonUtil.getSparkContext(JobContext.parallelization, "DataExhaust");
            try {
                execute(conf);
            } finally {
                CommonUtil.closeSparkContext();
            }
        } else {
            implicit val sparkContext: SparkContext = sc.getOrElse(null);
            execute(conf);
        }
    }

    def execute(config: PackagerConfig)(implicit sc: SparkContext): Array[Response] = {
        // Get all job request with status equals PENDING_PACKAGING
        val jobRequests = sc.cassandraTable[JobRequest](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST).where("status = ?", "PENDING_PACKAGING").collect;

        jobRequests.map { request =>
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

    def packageExhaustData(jobRequest: JobRequest, config: PackagerConfig)(implicit sc: SparkContext): Response = {

        val path = config.saveType match {
            case "s3" =>
                val local = config.localPath + "/" + jobRequest.request_id + "/"
                S3Util.download(config.bucket, config.prefix + jobRequest.request_id, local)
                local
            case "local" =>
                config.localPath + "/" + jobRequest.request_id
        }
        val fileObj = new File(path)
        val data = fileObj.listFiles.map { dir =>
            (dir.getName, getData(dir, jobRequest, path))
        }

        val metadata = generateManifestFile(data, jobRequest, config.localPath)
        generateDataFiles(data, jobRequest, config.localPath)

        val localPath = config.localPath + "/" + jobRequest.request_id
        CommonUtil.zipDir(localPath + ".zip", localPath)
        CommonUtil.deleteDirectory(localPath);

        val job_id = UUID.randomUUID().toString()
        val fileStats = config.saveType match {
            case "s3" =>
                val prefix = config.prefix + jobRequest.request_id
                DataExhaustUtils.uploadZip(config.bucket, config.prefix, localPath, jobRequest.request_id, jobRequest.client_key)
                val stats = S3Util.getObjectDetails(config.bucket, prefix + ".zip");
                (config.public_S3URL + "/" + config.bucket + "/" + prefix + ".zip", stats)
            case "local" =>
                val file = new File(localPath)
                val dateTime = new Date(file.lastModified())
                val stats = Map("createdDate" -> dateTime, "size" -> file.length())
                (localPath + ".zip", stats)

        }
        Response(jobRequest.request_id, jobRequest.client_key, job_id, metadata, fileStats._1, fileStats._2, jobRequest);
    }

    def generateManifestFile(data: Array[(String, Array[String])], jobRequest: JobRequest, localPath: String): ManifestFile = {
        val fileInfo = data.map { f =>
            val firstEvent = JSONUtils.deserialize[Map[String, AnyRef]](f._2.head)
            val lastEvent = JSONUtils.deserialize[Map[String, AnyRef]](f._2.last)
            FileInfo(f._1, f._2.length, CommonUtil.dateFormat.print(new DateTime(firstEvent.get("ets").get.asInstanceOf[Number].longValue())), CommonUtil.dateFormat.print(new DateTime(lastEvent.get("ets").get.asInstanceOf[Number].longValue())), 0.0)
        }
        val totalEventCount = data.map { f => f._2.length }.sum
        val requestData = JSONUtils.deserialize[Map[String, AnyRef]](jobRequest.request_data)
        val filter = requestData.get("filter").get.asInstanceOf[Map[String, AnyRef]]
        val manifest = ManifestFile("ekstep.analytics.dataset", "1.0", CommonUtil.dateFormat.print(new DateTime(System.currentTimeMillis())), requestData.get("dataset_id").get.toString(), totalEventCount, filter.get("startDate").toString(), filter.get("endDate").toString(), fileInfo, requestData)
        val gson = new GsonBuilder().setPrettyPrinting().create();
        val jsonString = gson.toJson(manifest)
        writeToFile(Array(jsonString), localPath + "/" + jobRequest.request_id + "/", "manifest.json")
        manifest
    }

    def generateDataFiles(data: Array[(String, Array[String])], jobRequest: JobRequest, localPath: String) {
        val fileExtenstion = JSONUtils.deserialize[RequestConfig](jobRequest.request_data).output_format.getOrElse("json").toLowerCase()
        data.foreach { events =>
            val fileName = events._1
            val fileData = events._2
            writeToFile(fileData, localPath + "/" + jobRequest.request_id + "/data/", fileName + "." + fileExtenstion)
            CommonUtil.deleteDirectory(localPath + "/" + jobRequest.request_id + "/" + fileName)
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