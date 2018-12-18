package org.ekstep.media.service.test

import org.ekstep.media.common.MediaRequest
import org.ekstep.media.service.impl.MediaServiceFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}

import scala.collection.immutable.HashMap

/**
  * AzureMediaServiceTest handles all Unit Test Cases for Azure Media Service
  *
  * @author Kumar Gauraw
  */
class AzureMediaServiceTest extends FlatSpec with Matchers with MockFactory with PrivateMethodTester {

  val service = MediaServiceFactory.getMediaService()

  "submit job with valid input" should "submit the encoding job to mediaconvert service" in {
    val artifactUrl: String = "https://s3.ap-south-1.amazonaws.com/ekstep-stream-test/content/do_5555/artifact/countdown_timer_kids.mp4"
    val submitResponse = service.submitJob(new MediaRequest("test_001", null, HashMap[String, AnyRef]("content_id" -> "MT_001", "artifact_url" -> artifactUrl)))
    //println("submitResponse : "+submitResponse)
    val result: Map[String, AnyRef] = submitResponse.result.getOrElse("job", Map).asInstanceOf[Map[String, AnyRef]]
    submitResponse.responseCode shouldEqual "OK"
    result.nonEmpty shouldEqual true
    result.getOrElse("id", "").toString should not be ""
    result.getOrElse("status", "").toString shouldEqual "QUEUED"
  }

}
