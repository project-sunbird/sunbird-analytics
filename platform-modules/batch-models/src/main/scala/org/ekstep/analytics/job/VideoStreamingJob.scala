package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.{IJob, JobConfig, JobContext}
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.util.JobRequest
import org.ekstep.analytics.videostream.VideoStreamingUtils

object VideoStreamingJob extends optional.Application with IJob {

  implicit val className = "org.ekstep.analytics.job.VideoStreamingJob"
  def name: String = "VideoStreamingJob"

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

      JobLogger.end("VideoStreaming Job Completed.", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> 0, "outputEvents" -> 0, "timeTaken" -> 0, "jobCount" -> 0, "requestDetails" -> null)))
  }

  private def _submitJobRequests(submitted: RDD[JobRequest], config: JobConfig)(implicit sc: SparkContext) = {

  }

  private def _getCompletedRequests(processing: RDD[JobRequest], config: JobConfig)(implicit sc: SparkContext) = {

  }




}