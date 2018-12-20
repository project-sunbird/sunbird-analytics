package org.ekstep.media.service.impl

import org.ekstep.media.common._
import org.ekstep.media.config.AppConfig
import org.ekstep.media.service.AWSMediaService
import org.ekstep.media.util.HttpRestUtil
import scala.collection.immutable.HashMap

/**
  *
  * @author gauraw
  */
object AWSMediaServiceImpl extends AWSMediaService {

  override def submitJob(jobRequest: MediaRequest): MediaResponse = {
    try {
      val url = getApiUrl("job")
      val reqBody = prepareJobRequestBody(jobRequest.request)
      val header = getDefaultHeader("POST", url, reqBody)
      val response = HttpRestUtil.post(url, header, reqBody)
      if (response.responseCode == "OK") Response.getSuccessResponse(AWSResult.getSubmitJobResult(response)) else response
    } catch {
      case e: Exception => Response.getFailureResponse(new HashMap[String, AnyRef], "SERVER_ERROR", "Something Went Wrong While Processing Your Request.")
    }
  }

  override def getJob(jobId: String): MediaResponse = {
    try {
      val response = getJobDetails(jobId)
      if (response.responseCode == "OK") Response.getSuccessResponse(AWSResult.getJobResult(response)) else response
    } catch {
      case e: Exception => Response.getFailureResponse(new HashMap[String, AnyRef], "SERVER_ERROR", "Something Went Wrong While Processing Your Request.")
    }
  }

  override def getStreamingPaths(jobId: String): MediaResponse = {
    try {
      val getResponse = getJobDetails(jobId)
      if (getResponse.responseCode != "OK")
        return getResponse
      val region = AppConfig.getConfig("aws.region");
      val streamType = AppConfig.getConfig("aws.stream.protocol").toLowerCase()
      val inputs: List[Map[String, AnyRef]] = getResponse.result.getOrElse("job", Map).asInstanceOf[Map[String, AnyRef]].getOrElse("settings", Map).asInstanceOf[Map[String, AnyRef]].getOrElse("inputs", List).asInstanceOf[List[Map[String, AnyRef]]]
      val input: String = inputs.head.getOrElse("fileInput", "").toString
      val host = "https://s3." + region + ".amazonaws.com"
      val streamUrl: String = input.replace("s3:/", host)
        .replace("artifact", streamType)
        .replace(".mp4", ".m3u8")
      Response.getSuccessResponse(HashMap[String, AnyRef]("streamUrl" -> streamUrl))
    } catch {
      case e: Exception => Response.getFailureResponse(new HashMap[String, AnyRef], "SERVER_ERROR", "Something Went Wrong While Processing Your Request.")
    }
  }

  override def listJobs(listJobsRequest: MediaRequest): MediaResponse = {
    null
  }

  override def cancelJob(cancelJobRequest: MediaRequest): MediaResponse = {
    null
  }

}
