package org.ekstep.analytics.videostream

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.util.{Constants, JobRequest, JobStage}
import com.datastax.spark.connector._

object VideoStreamingUtils {

  def getAllRequest(maxIterations: Int)(implicit sc: SparkContext): RDD[JobRequest] = {
      try {
          val jobReq = sc.cassandraTable[JobRequest](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST).filter { x => x.job_name.getOrElse("").equals("VIDEO_STREAMING") }.filter{ x => x.status.equals("SUBMITTED") || (x.status.equals("FAILED") && x.iteration.getOrElse(0) < maxIterations)}.cache;
          if (!jobReq.isEmpty()) {
            jobReq.map { x => JobStage(x.request_id, x.client_key, "FETCHING_ALL_REQUEST", "COMPLETED", "PROCESSING") }.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("request_id", "client_key", "stage", "stage_status", "status", "err_message", "dt_job_processing"))
            jobReq;
          } else {
            null;
          }
      } catch {
          case t: Throwable => null;
      }
  }

}
