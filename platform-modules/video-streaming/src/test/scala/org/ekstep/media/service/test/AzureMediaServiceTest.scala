package org.ekstep.media.service.test

import org.ekstep.media.common.MediaRequest
import org.ekstep.media.service.impl.MediaServiceFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.HashMap

/**
  * AzureMediaServiceTest handles all Unit/Functional Test Cases for Azure Media Service
  *
  * @author Kumar Gauraw
  */
class AzureMediaServiceTest extends FlatSpec with Matchers {

  val service = MediaServiceFactory.getMediaService()

  "submit job with valid input" should "submit the encoding job to azure media service" in {
    val artifactUrl: String = "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_112658936445755392160/artifact/small_1545280511077.mp4"
    val submitResponse = service.submitJob(new MediaRequest("ft_test_001", null, HashMap[String, AnyRef]("content_id" -> "FT_MS_", "artifact_url" -> artifactUrl)))
    val result: Map[String, AnyRef] = submitResponse.result.getOrElse("job", Map).asInstanceOf[Map[String, AnyRef]]
    submitResponse.responseCode shouldEqual "OK"
    result.nonEmpty shouldEqual true
    result.getOrElse("id", "").toString should not be ""
    result.getOrElse("status", "").toString shouldEqual "QUEUED"
  }

  "submit job with invalid artifact url" should "give CLIENT_ERROR Response" in {
    val artifactUrl: String = "sunbirddev.blob.core.windows.net/sunbird-content-dev/do_112658936445755392160/artifact/small_1545280511077.mp4"
    val submitResponse = service.submitJob(new MediaRequest("ft_test_001", null, HashMap[String, AnyRef]("content_id" -> "FT_MS_", "artifact_url" -> artifactUrl)))
    val params: Map[String, AnyRef] = submitResponse.params.asInstanceOf[Map[String, AnyRef]]
    submitResponse.responseCode shouldEqual "CLIENT_ERROR"
    submitResponse.result.nonEmpty shouldEqual true
    params.getOrElse("err", "") shouldEqual "BAD_REQUEST"
    params.getOrElse("errMsg", "") shouldEqual "Please Provide Correct Request Data."
  }

  "get job with valid job id" should "give the azure media service job details" in {
    val jobId: String = "FT_MS__1545291549632"
    val getResponse = service.getJob(jobId)
    val result: Map[String, AnyRef] = getResponse.result.getOrElse("job", Map).asInstanceOf[Map[String, AnyRef]]
    getResponse.responseCode shouldEqual "OK"
    result.nonEmpty shouldEqual true
    result.getOrElse("id", "").toString shouldEqual jobId
    result.getOrElse("status", "").toString.nonEmpty shouldEqual true
    result.getOrElse("submittedOn", "").toString.nonEmpty shouldEqual true
  }

  "get job with invalid job id" should "give RESOURCE_NOT_FOUND Response" in {
    val jobId: String = "FT_MS_1234"
    val getResponse = service.getJob(jobId)
    val params: Map[String, AnyRef] = getResponse.params.asInstanceOf[Map[String, AnyRef]]
    getResponse.responseCode shouldEqual "RESOURCE_NOT_FOUND"
    params.nonEmpty shouldEqual true
    getResponse.result.nonEmpty shouldEqual true
    params.getOrElse("err", "") shouldEqual "RESOURCE_NOT_FOUND"
    params.getOrElse("errMsg", "") shouldEqual "Resource Not Found."
  }

  "get streaming path with valid job id" should "give valid streaming path" in {
    val jobId: String = "FT_MS__1545291549632"
    val getPathResponse = service.getStreamingPaths(jobId)
    getPathResponse.responseCode shouldEqual "OK"
    getPathResponse.result.nonEmpty shouldEqual true
    getPathResponse.result.getOrElse("streamUrl", "").toString endsWith "(format=m3u8-aapl-v3)"
  }

  "get streaming path with invalid job id" should "give RESOURCE_NOT_FOUND Response" in {
    val jobId: String = "FT_MS_1234"
    val getPathResponse = service.getStreamingPaths(jobId)
    val params: Map[String, AnyRef] = getPathResponse.params.asInstanceOf[Map[String, AnyRef]]
    getPathResponse.responseCode shouldEqual "RESOURCE_NOT_FOUND"
    params.nonEmpty shouldEqual true
    getPathResponse.result.nonEmpty shouldEqual true
    params.getOrElse("err", "") shouldEqual "RESOURCE_NOT_FOUND"
    params.getOrElse("errMsg", "") shouldEqual "Resource Not Found."
  }

}
