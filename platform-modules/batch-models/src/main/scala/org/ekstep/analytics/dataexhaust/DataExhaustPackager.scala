/**
 * @author Jitendra Singh Sankhwar, Mahesh, Amit
 */
package org.ekstep.analytics.dataexhaust

import java.io.File
import java.io.FileWriter
import java.io.PrintWriter
import java.util.Date
import java.util.UUID

import scala.reflect.runtime.universe

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.OutputDispatcher
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

/**
 * Case class to hold the manifest json 
 */
case class FileInfo(path: String, event_count: Long, first_event_date: String, last_event_date: String)
case class ManifestFile(id: String, ver: String, ets: Long, request_id: String, dataset_id: String, total_event_count: Long, first_event_date: Long, last_event_date: Long, file_info: Array[FileInfo], request: String)
case class Response(request_id: String, client_key: String, job_id: String, metadata: ManifestFile, location: String, stats: Map[String, Any], jobRequest: JobRequest)

/**
 * @Packager
 *
 * DataExhaustPackager
 *
 * Functionality
 * 1. Package the all the events based upon their identifer either in json or csv format
 * Events used - consumption-raw, consumption-summary, eks-consumption-metrics, creation-raw, eks-creation-summary and eks-creation-metrics
 */
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
			val executionTime = x.jobRequest.execution_time;
			JobRequest(x.client_key, x.request_id, Option(x.job_id), "COMPLETED", x.jobRequest.request_data, Option(x.location),
				Option(createdDate),
				Option(new DateTime(CommonUtil.dateFormat.parseDateTime(first_event_date).getMillis)),
				Option(new DateTime(CommonUtil.dateFormat.parseDateTime(last_event_date).getMillis)),
				Option(createdDate.plusDays(30)), Option(x.jobRequest.iteration.getOrElse(0) + 1), x.jobRequest.dt_job_submitted, Option(dtProcessing), Option(DateTime.now(DateTimeZone.UTC)),
				None, Option(x.metadata.total_event_count), Option(x.stats.get("size").get.asInstanceOf[Long]), Option(0), executionTime, None, Option("UPDATE_RESPONSE_TO_DB"), Option("COMPLETED"));
		}
		sc.makeRDD(jobResuestStatus).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST);
	}
	
	/**
     * To filter the invalid files
     * @param filePath  
     * @return true if file is valid and false for invalid file 
     */
	private def invalidFileFilter(filePath: String): Boolean = {
		filePath.endsWith("_SUCCESS") || filePath.endsWith("$folder$") || filePath.endsWith(".crc");
	}
	
	/**
     * Read all file from a directory and merge into one RDD Type
     * @param dir file directory 
     * @return RDD[String] of merge files
     */
	def getData(dir: File)(implicit sc: SparkContext): RDD[String] = {
		val data = dir.listFiles().map { x =>
            sc.textFile(x.getAbsolutePath)
        }
        sc.union(data.toSeq)
	}
	
	/**
     * package all the data according to the request and return the response
     * @param jobRequest request for data-exhaust 
     * @return Response for the request
     */
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
	
	/**
     * Delete all invalid files in a given file directory
     * @param path directory path 
     */
	private def deleteInvalidFiles(path: String) {
		val fileObj = new File(path);
		fileObj.listFiles.map { x => if (x.isDirectory()) deleteInvalidFiles(x.getAbsolutePath); x }
		.filter { x => invalidFileFilter(x.getAbsolutePath) }
		.map { x => x.delete() };
	}
	
	/**
     * Generate manifest json file and save into the request folder struture.
     * @param data Array of all merge data with respect to each event identifier 
     * @param jobRequest request for data-exhaust
     * @param requestDataLocalPath path of the events file  
     * @return manifest case class 
     */
	def generateManifestFile(data: Array[(String, RDD[String])], jobRequest: JobRequest, requestDataLocalPath: String)(implicit sc: SparkContext): ManifestFile = {
		val fileExtenstion = JSONUtils.deserialize[RequestConfig](jobRequest.request_data).output_format.getOrElse("json").toLowerCase()
	    val fileInfo = data.map { f =>
			val firstEvent = JSONUtils.deserialize[Map[String, AnyRef]](f._2.first())
			val lastEvent = JSONUtils.deserialize[Map[String, AnyRef]](f._2.take(f._2.count().toInt).head)
			FileInfo("data/" + f._1 + "." + fileExtenstion, f._2.count, CommonUtil.dateFormat.print(new DateTime(firstEvent.get("ets").get.asInstanceOf[Number].longValue())), CommonUtil.dateFormat.print(new DateTime(lastEvent.get("ets").get.asInstanceOf[Number].longValue())))
		}
		val events = sc.union(data.map { event => event._2 }.toSeq).sortBy { x => JSONUtils.deserialize[Map[String, AnyRef]](x).get("ets").get.asInstanceOf[Number].longValue() }
		val totalEventCount = jobRequest.output_events.get;
		val requestData = JSONUtils.deserialize[Map[String, AnyRef]](jobRequest.request_data)
		val firstEventDate = jobRequest.dt_first_event.get.getMillis;
		val lastEventDate = jobRequest.dt_last_event.get.getMillis;
		val manifest = ManifestFile("ekstep.analytics.dataset", "1.0", new Date().getTime, jobRequest.request_id, requestData.get("dataset_id").get.toString(), totalEventCount, firstEventDate, lastEventDate, fileInfo, JSONUtils.serialize(requestData))
		val gson = new GsonBuilder().setPrettyPrinting().create();
		val jsonString = gson.toJson(manifest)
		val outputManifestPath = requestDataLocalPath + "manifest.json"
		OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> outputManifestPath)), Array(jsonString));
		manifest
	}
	
	/**
     * Generate manifest json file and save into the request folder struture.
     * @param data Array of all merge data with respect to each event identifier 
     * @param jobRequest request for data-exhaust
     * @param requestDataLocalPath path of the manifest file  
     */
	def generateDataFiles(data: Array[(String, RDD[String])], jobRequest: JobRequest, requestDataLocalPath: String)(implicit sc: SparkContext) {
		val fileExtenstion = JSONUtils.deserialize[RequestConfig](jobRequest.request_data).output_format.getOrElse("json").toLowerCase()
		data.foreach { events =>
			val fileName = events._1
			val fileData = events._2
			val path = requestDataLocalPath + "data/" + fileName + "." + fileExtenstion
            OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> path)), fileData);
			CommonUtil.deleteDirectory(requestDataLocalPath + fileName)
		}
	}
}