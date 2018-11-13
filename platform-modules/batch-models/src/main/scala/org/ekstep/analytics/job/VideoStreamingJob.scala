package org.ekstep.analytics.job

import java.util.UUID

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{IJob, JobConfig, JobContext}
import org.ekstep.analytics.util.{Constants, JobRequest, JobStage}
import org.ekstep.analytics.videostream.VideoStreamingUtils
import org.sunbird.media.common.{MediaRequest, MediaResponse}
import org.sunbird.media.service.impl.MediaServiceFactory
import com.datastax.spark.connector._
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

object VideoStreamingJob extends optional.Application with IJob {

  implicit val className = "org.ekstep.analytics.job.VideoStreamingJob"
  def name: String = "VideoStreamingJob"
  val mediaService = MediaServiceFactory.getMediaService()

  case class StreamingStage(request_id: String, client_key: String, job_id: String, stage: String, stage_status: String, status: String, iteration: Int, err_message: String = "")

  def main(config: String)(implicit sc: Option[SparkContext] = None): Unit = {
    JobLogger.init(name)
    JobLogger.start("VideoStreaming Job Started executing", Option(Map("config" -> config, "model" -> name)))
    val jobConfig = JSONUtils.deserialize[JobConfig](config)

    if (null == sc.getOrElse(null)) {
      JobContext.parallelization = 10
      implicit val sparkContext = CommonUtil.getSparkContext(JobContext.parallelization, jobConfig.appName.getOrElse(jobConfig.model))
      try {
        execute(jobConfig)
      } finally {
        CommonUtil.closeSparkContext()
      }
    } else {
      implicit val sparkContext: SparkContext = sc.getOrElse(null)
      execute(jobConfig)
    }
  }

  private def execute(config: JobConfig)(implicit sc: SparkContext) = {
      val requests = VideoStreamingUtils.getAllRequest(10)
      if (null != requests) {
          _executeRequests(requests, config)
      } else {
        JobLogger.end("VideoStreaming Job Completed. But There is no job request in DB", "SUCCESS", Option(Map("model" -> name, "date" -> "", "timeTaken" -> 0)))
      }
  }

  private def _executeRequests(requests: RDD[JobRequest], config: JobConfig)(implicit sc: SparkContext) = {
      // TODO: add the implementation of Request VideoStreaming
    val submitted = requests.filter(r => List("SUBMITTED", "FAILED").contains(r.status)).cache()
    val processing = requests.filter(r => "PROCESSING".contentEquals(r.status)).cache()

    val submittedResponse = _submitJobRequests(submitted, config)
    _getCompletedRequests(processing, config)
      JobLogger.end("VideoStreaming Job Completed.", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> 0, "outputEvents" -> 0, "timeTaken" -> 0, "jobCount" -> 0, "requestDetails" -> null)))
  }

  private def _submitJobRequests(submitted: RDD[JobRequest], config: JobConfig)(implicit sc: SparkContext): Unit = {
    val stageName = "STREAMING_JOB_SUBMISSION";
    submitted.map { jobRequest =>
      val mediaRequest = MediaRequest(UUID.randomUUID().toString, null, JSONUtils.deserialize[Map[String,AnyRef]](jobRequest.request_data.replaceAll("artifactUrl","artifact_url")))
      val response = mediaService.submitJob(mediaRequest)
      (jobRequest, response)
    }.map {
      x =>
        val request = x._1
        val response = x._2
        val iteration = request.iteration.getOrElse(0)
        if (response.responseCode.equals("OK")) {
            val jobId = response.result.getOrElse("job", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("id","").asInstanceOf[String];
            val jobStatus = response.result.getOrElse("job", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("status","").asInstanceOf[String];
            StreamingStage(request.request_id, request.client_key, jobId, stageName, jobStatus, "PROCESSING", iteration);
        } else {
            val errorMsg = JSONUtils.serialize(response.result)
            StreamingStage(request.request_id, request.client_key, null, stageName, "FAILED", "FAILED", iteration + 1, errorMsg);
        }
    }.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("job_id", "stage", "stage_status", "status", "iteration", "err_message"))
  }

  private def _getCompletedRequests(processing: RDD[JobRequest], config: JobConfig)(implicit sc: SparkContext) = {

    processing.foreach(jobRequest => {
      val mediaResponse:MediaResponse = mediaService.getJob(jobRequest.job_id.get)

    })
  }




}