package org.ekstep.analytics.dataexhaust

import java.io.File
import java.io.FileWriter
import java.io.FilenameFilter
import java.io.PrintWriter
import java.util.Date
import java.util.UUID

import scala.reflect.runtime.universe

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.S3Util
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.util.JobRequest
import org.ekstep.analytics.util.RequestConfig
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.toSparkContextFunctions
import com.google.gson.GsonBuilder

case class PackagerConfig(saveType: String, bucket: String, prefix: String, public_S3URL: String, localPath: String)
case class FileInfo(event_id: String, event_count: Long, first_event_date: String, last_event_date: String, file_size: Double)
case class ManifestFile(id: String, ver: String, ts: String, dataset_id: String, total_event_count: Long, start_date: String, end_end: String, file_info: Array[FileInfo], request: Map[String, AnyRef])
case class Response(request_id: String, client_key: String, job_id: String, metadata: ManifestFile, location: String, stats: Map[String, Any], jobRequest: JobRequest)

object DataExhaustPackager extends optional.Application {

    val className = "org.ekstep.analytics.model.DataExhaustPackager"
    def name: String = "DataExhaustPackager"

    def execute()(implicit sc: SparkContext) = {
        // Get all job request with status equals PENDING_PACKAGING
        val jobRequests = sc.cassandraTable[JobRequest](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST).where("status = ?", "PENDING_PACKAGING").collect;

        val metadata = jobRequests.map { request =>
            packageExhaustData(request)
        }
        val jobResuestStatus = metadata.map { x =>
            val createdDate = new DateTime(x.stats.get("createdDate").get.asInstanceOf[Date].getTime);
            val fileInfo = x.metadata.file_info.sortBy { x => x.first_event_date }
            val first_event_date = fileInfo.head.first_event_date
            val last_event_date = fileInfo.last.last_event_date
            val dtProcessing = DateTime.now(DateTimeZone.UTC);
            JobRequest(x.client_key, x.request_id, Option(x.job_id), "COMPLETED", x.jobRequest.request_data, Option(x.location),
                Option(createdDate),
                Option(new DateTime(CommonUtil.dateFormat.parseDateTime(first_event_date).getMillis)),
                Option(new DateTime(CommonUtil.dateFormat.parseDateTime(last_event_date).getMillis)),
                Option(createdDate.plusDays(30)), Option(x.jobRequest.iteration.getOrElse(0) + 1), x.jobRequest.dt_job_submitted, Option(dtProcessing), Option(DateTime.now(DateTimeZone.UTC)),
                None, Option(x.metadata.total_event_count), Option(x.stats.get("size").get.asInstanceOf[Long]), Option(0), None, None, Option("UPDATE_RESPONSE_TO_DB"), Option("COMPLETED"));
        }
        sc.makeRDD(jobResuestStatus).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST);
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

    def packageExhaustData(jobRequest: JobRequest)(implicit sc: SparkContext): Response = {
        
        val saveType = AppConf.getConfig("dataexhaust.save_config.save_type")
        val bucket = AppConf.getConfig("dataexhaust.save_config.bucket")
        val prefix = AppConf.getConfig("dataexhaust.save_config.prefix")
        val tmpPath = AppConf.getConfig("dataexhaust.save_config.local_path")
        val public_S3URL = AppConf.getConfig("dataexhaust.save_config.public_s3_url")
        
        val path = saveType match {
            case "s3" =>
                S3Util.download(bucket, prefix + jobRequest.request_id, tmpPath + "/")
                (tmpPath + "/" + jobRequest.request_id + "/")
            case "local" =>
                tmpPath + "/" + jobRequest.request_id + "/"
        }
        val fileObj = new File(path.toString())
        val data = fileObj.listFiles.map { dir =>
            (dir.getName, getData(dir, jobRequest, path))
        }

        val metadata = generateManifestFile(data, jobRequest, tmpPath)
        generateDataFiles(data, jobRequest, tmpPath)

        val localPath = tmpPath + "/" + jobRequest.request_id
        CommonUtil.zipDir(localPath + ".zip", localPath)
        CommonUtil.deleteDirectory(localPath);

        val job_id = UUID.randomUUID().toString()
        val fileStats = saveType match {
            case "s3" =>
                val s3Prefix = prefix + jobRequest.request_id
                DataExhaustUtils.uploadZip(bucket, s3Prefix, localPath, jobRequest.request_id, jobRequest.client_key)
                val stats = S3Util.getObjectDetails(bucket, s3Prefix + ".zip");
                (public_S3URL + "/" + bucket + "/" + s3Prefix + ".zip", stats)
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