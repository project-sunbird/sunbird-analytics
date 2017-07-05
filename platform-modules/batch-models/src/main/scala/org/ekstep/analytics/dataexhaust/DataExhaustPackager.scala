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
import org.apache.commons.io.FileUtils

case class PackagerConfig(saveType: String, bucket: String, prefix: String, public_S3URL: String, localPath: String)
case class FileInfo(event_id: String, event_count: Long, first_event_date: String, last_event_date: String)
case class ManifestFile(id: String, ver: String, ets: Long, request_id: String, dataset_id: String, total_event_count: Long, start_date: String, end_end: String, file_info: Array[FileInfo], request: String)
case class Response(request_id: String, client_key: String, job_id: String, metadata: ManifestFile, location: String, stats: Map[String, Any], jobRequest: JobRequest)

object DataExhaustPackager extends optional.Application {

	val className = "org.ekstep.analytics.model.DataExhaustPackager"
	def name: String = "DataExhaustPackager"

	def execute()(implicit sc: SparkContext) = {

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

	private def invalidFileFilter(filePath: String): Boolean = {
		filePath.endsWith("_SUCCESS") || filePath.endsWith("$folder$") || filePath.endsWith(".crc");
	}
	
	def getData(dir: File)(implicit sc: SparkContext): Array[String] = {
		dir.listFiles().map { x =>
			sc.textFile(x.getAbsolutePath)
		}.flatMap { x => x.collect() }
	}

	def packageExhaustData(jobRequest: JobRequest)(implicit sc: SparkContext): Response = {

		val saveType = AppConf.getConfig("data_exhaust.save_config.save_type")
		val bucket = AppConf.getConfig("data_exhaust.save_config.bucket")
		val prefix = AppConf.getConfig("data_exhaust.save_config.prefix")
		val tmpFolderPath = AppConf.getConfig("data_exhaust.save_config.local_path")
		val public_S3URL = AppConf.getConfig("data_exhaust.save_config.public_s3_url")
		val deleteSource = AppConf.getConfig("data_exhaust.delete_source");
		
		val requestDataLocalPath = tmpFolderPath + jobRequest.request_id + "/";
		val requestDataSourcePath = prefix + jobRequest.request_id + "/";
		val zipFileAbsolutePath = tmpFolderPath + jobRequest.request_id + ".zip";
		
		saveType match {
			case "s3" =>
				S3Util.downloadDirectory(bucket, requestDataSourcePath, requestDataLocalPath)
			case "local" =>
				FileUtils.copyDirectory(new File(requestDataSourcePath), new File(requestDataLocalPath));
		}
		deleteInvalidFiles(requestDataLocalPath);
		
		val fileObj = new File(requestDataLocalPath)
		val data = fileObj.listFiles.map { dir =>
			(dir.getName, getData(dir))
		}

		val metadata = generateManifestFile(data, jobRequest, requestDataLocalPath)
		generateDataFiles(data, jobRequest, requestDataLocalPath)
		val localPath = tmpFolderPath + jobRequest.request_id
		CommonUtil.zipDir(zipFileAbsolutePath, requestDataLocalPath)
		CommonUtil.deleteDirectory(requestDataLocalPath);

		val job_id = UUID.randomUUID().toString()
		val fileStats = saveType match {
			case "s3" =>
				val s3FilePrefix = prefix + jobRequest.request_id +".zip"
				DataExhaustUtils.uploadZip(bucket, s3FilePrefix, zipFileAbsolutePath, jobRequest.request_id, jobRequest.client_key)
				val stats = S3Util.getObjectDetails(bucket, s3FilePrefix);
				if ("true".equals(deleteSource)) DataExhaustUtils.deleteS3File(bucket, prefix, Array(jobRequest.request_id))
				(public_S3URL + "/" + bucket + "/" + s3FilePrefix, stats)
			case "local" =>
				val file = new File(localPath)
				val dateTime = new Date(file.lastModified())
				val stats = Map("createdDate" -> dateTime, "size" -> file.length())
				if ("true".equals(deleteSource)) CommonUtil.deleteDirectory(prefix + jobRequest.request_id);
				(localPath + ".zip", stats)
		}

		Response(jobRequest.request_id, jobRequest.client_key, job_id, metadata, fileStats._1, fileStats._2, jobRequest);
	}
	
	private def deleteInvalidFiles(path: String) {
		val fileObj = new File(path);
		fileObj.listFiles.map { x => if (x.isDirectory()) deleteInvalidFiles(x.getAbsolutePath); x }
		.filter { x => invalidFileFilter(x.getAbsolutePath) }
		.map { x => x.delete() };
	}

	def generateManifestFile(data: Array[(String, Array[String])], jobRequest: JobRequest, requestDataLocalPath: String): ManifestFile = {
		val fileInfo = data.map { f =>
			val firstEvent = JSONUtils.deserialize[Map[String, AnyRef]](f._2.head)
			val lastEvent = JSONUtils.deserialize[Map[String, AnyRef]](f._2.last)
			FileInfo(f._1, f._2.length, CommonUtil.dateFormat.print(new DateTime(firstEvent.get("ets").get.asInstanceOf[Number].longValue())), CommonUtil.dateFormat.print(new DateTime(lastEvent.get("ets").get.asInstanceOf[Number].longValue())))
		}
		val events = data.map { event => event._2 }.flatten.sortBy { x => JSONUtils.deserialize[Map[String, AnyRef]](x).get("ets").get.asInstanceOf[Number].longValue() }
		val firstEvent = JSONUtils.deserialize[Map[String, AnyRef]](events.head)
		val lastEvent = JSONUtils.deserialize[Map[String, AnyRef]](events.last)
		val totalEventCount = data.map { f => f._2.length }.sum
		val requestData = JSONUtils.deserialize[Map[String, AnyRef]](jobRequest.request_data)
		val manifest = ManifestFile("ekstep.analytics.dataset", "1.0", new Date().getTime, jobRequest.request_id, requestData.get("dataset_id").get.toString(), totalEventCount, CommonUtil.dateFormat.print(new DateTime(firstEvent.get("ets").get.asInstanceOf[Number].longValue())), CommonUtil.dateFormat.print(new DateTime(lastEvent.get("ets").get.asInstanceOf[Number].longValue())), fileInfo, JSONUtils.serialize(requestData))
		val gson = new GsonBuilder().setPrettyPrinting().create();
		val jsonString = gson.toJson(manifest)
		writeToFile(Array(jsonString), requestDataLocalPath, "manifest.json")
		manifest
	}

	def generateDataFiles(data: Array[(String, Array[String])], jobRequest: JobRequest, requestDataLocalPath: String) {
		val fileExtenstion = JSONUtils.deserialize[RequestConfig](jobRequest.request_data).output_format.getOrElse("json").toLowerCase()
		data.foreach { events =>
			val fileName = events._1
			val fileData = events._2
			writeToFile(fileData, requestDataLocalPath + "data/", fileName + "." + fileExtenstion)
			CommonUtil.deleteDirectory(requestDataLocalPath + fileName)
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