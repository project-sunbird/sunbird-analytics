package org.ekstep.media.service.test

import org.ekstep.media.common.MediaRequest
import org.ekstep.media.service.impl.MediaServiceFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.HashMap

/**
  * AwsMediaServiceTest handles all Unit/Functional Test Cases for AWS MediaConvert Service
  *
  * @author Kumar Gauraw
  */
class AwsMediaServiceTest extends FlatSpec with Matchers {

  val service = MediaServiceFactory.getMediaService()

  "submit job with valid input" should "submit the encoding job to aws mediaconvert service" in {
    val artifactUrl: String = "https://ekstep-public-dev.s3-ap-south-1.amazonaws.com/content/do_11265914204844032012897/artifact/small_1545305482297.mp4"
    val submitResponse = service.submitJob(new MediaRequest("ft_test_001", null, HashMap[String, AnyRef]("content_id" -> "FT_MS_001", "artifact_url" -> artifactUrl)))
    val result: Map[String, AnyRef] = submitResponse.result.getOrElse("job", Map).asInstanceOf[Map[String, AnyRef]]
    submitResponse.responseCode shouldEqual "OK"
    result.nonEmpty shouldEqual true
    result.getOrElse("id", "").toString should not be ""
    result.getOrElse("status", "").toString shouldEqual "SUBMITTED"
  }

  "submit job with invalid artifact url" should "give SERVER_ERROR Response" in {
    val artifactUrl: String = "https://ekstep-public-dev.s3-ap-south-1.amazonaws.com/do_123/artifact/small_1545305482297.mp4"
    val submitResponse = service.submitJob(new MediaRequest("ft_test_001", null, HashMap[String, AnyRef]("content_id" -> "FT_MS_", "artifact_url" -> artifactUrl)))
    val params: Map[String, AnyRef] = submitResponse.params.asInstanceOf[Map[String, AnyRef]]
    submitResponse.responseCode shouldEqual "SERVER_ERROR"
    submitResponse.result.nonEmpty shouldEqual false
    params.getOrElse("err", "") shouldEqual "SERVER_ERROR"
    params.getOrElse("errMsg", "") shouldEqual "Something Went Wrong While Processing Your Request."
  }

  "get job with valid job id" should "give the azure media service job details" in {
    val jobId: String = "1545305929208-g3sp1x"
    val getResponse = service.getJob(jobId)
    val result: Map[String, AnyRef] = getResponse.result.getOrElse("job", new HashMap[String,AnyRef]).asInstanceOf[Map[String, AnyRef]]
    getResponse.responseCode shouldEqual "OK"
    result.nonEmpty shouldEqual true
    result.getOrElse("id", "").toString shouldEqual jobId
    result.getOrElse("status", "").toString.nonEmpty shouldEqual true
    result.getOrElse("submittedOn", "").toString.nonEmpty shouldEqual true
  }

  "get job with invalid job id" should "give CLIENT_ERROR Response" in {
    val jobId: String = "FT_MS_1234"
    val getResponse = service.getJob(jobId)
    val params: Map[String, AnyRef] = getResponse.params.asInstanceOf[Map[String, AnyRef]]
    getResponse.responseCode shouldEqual "CLIENT_ERROR"
    params.nonEmpty shouldEqual true
    getResponse.result.nonEmpty shouldEqual true
    params.getOrElse("err", "") shouldEqual "BAD_REQUEST"
    params.getOrElse("errMsg", "") shouldEqual "Please Provide Correct Request Data."
  }

  "get streaming path with valid job id" should "give valid streaming path" in {
    val jobId: String = "1545305929208-g3sp1x"
    val getPathResponse = service.getStreamingPaths(jobId)
    getPathResponse.responseCode shouldEqual "OK"
    getPathResponse.result.nonEmpty shouldEqual true
    getPathResponse.result.getOrElse("streamUrl", "").toString endsWith ".m3u8"
  }

  "get streaming path with invalid job id" should "give CLIENT_ERROR Response" in {
    val jobId: String = "FT_MS_1234"
    val getPathResponse = service.getStreamingPaths(jobId)
    val params: Map[String, AnyRef] = getPathResponse.params.asInstanceOf[Map[String, AnyRef]]
    getPathResponse.responseCode shouldEqual "CLIENT_ERROR"
    params.nonEmpty shouldEqual true
    getPathResponse.result.nonEmpty shouldEqual true
    params.getOrElse("err", "") shouldEqual "BAD_REQUEST"
    params.getOrElse("errMsg", "") shouldEqual "Please Provide Correct Request Data."
  }
}
