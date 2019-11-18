package org.ekstep.analytics.api.service

import scala.collection.immutable.List

import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.api._
import org.ekstep.analytics.api.util._
import org.ekstep.analytics.framework.conf.AppConf
import org.joda.time.DateTime
import org.joda.time.LocalDate

class TestJobAPIService extends BaseSpec {

  "JobAPIService" should "return response for data request" in {
    val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"output_format": "json", "filter":{"start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]}}}"""
    val response = JobAPIService.dataRequest(request, "in.ekstep")
    println(response)
    response.responseCode should be("OK")
  }

  "JobAPIService" should "return success response for data request with type as json without dataset_id, app_id & channel" in {
    val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"output_format": "json", "filter":{"start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]}}}"""
    val response = JobAPIService.dataRequest(request, "in.ekstep")
    response.params.status should be("successful")

  }

  "JobAPIService" should "return success response for data request with dataset_id, app_id & channel" in {
    val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"output_format": "json", "dataset_id": "eks-consumption-raw", "filter":{"start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"], "app_id": "Ekstep", "channel": "KAR"}}}"""
    val response = JobAPIService.dataRequest(request, "in.ekstep")

    response.params.status should be("successful")

  }

  "JobAPIService" should "return success response for data request with type as csv and events size is one" in {
    val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"output_format": "csv", "filter":{"events":["OE_ASSESS"], "start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]}}}"""
    val response = JobAPIService.dataRequest(request, "in.ekstep")

    response.params.status should be("successful")

  }

  "JobAPIService" should "return failed response for data request with type as csv and events size is not equals to one" in {
    val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"output_format": "csv", "filter":{"events":["OE_ASSESS", "OE_START"], "start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]}}}"""
    val response = JobAPIService.dataRequest(request, "in.ekstep")

    response.params.status should be("failed")

  }

  "JobAPIService" should "return response for data request without type attribute" in {
    val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"filter":{"events":["OE_ASSESS"], "start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]}}}"""
    val response = JobAPIService.dataRequest(request, "in.ekstep")

    response.params.status should be("successful")
  }

  "JobAPIService" should "return response for data request with type as csv and events is not defined" in {
    val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"output_format": "csv", "filter":{"start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]}}}"""
    val response = JobAPIService.dataRequest(request, "in.ekstep")

    response.params.status should be("failed")
  }

  "JobAPIService" should "submit the failed request for retry" in {
    val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"filter":{"events":["OE_ASSESS"], "start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]}}}"""
    val response = JobAPIService.dataRequest(request, "in.ekstep")

    val requestId = response.result.getOrElse(Map()).getOrElse("request_id", "").asInstanceOf[String]
    StringUtils.isNotEmpty(requestId) should be(true)

    DBUtil.session.execute("UPDATE " + AppConf.getConfig("application.env") + "_platform_db.job_request SET status='FAILED' WHERE client_key='dev-portal' AND request_id='" + requestId + "'")
    val getResponse = JobAPIService.getDataRequest("dev-portal", requestId)
    val failStatus = getResponse.result.getOrElse(Map()).getOrElse("status", "").asInstanceOf[String]
    StringUtils.isNotEmpty(failStatus) should be(true)
    failStatus should be("FAILED")
    val response2 = JobAPIService.dataRequest(request, "in.ekstep")
    val status = response.result.getOrElse(Map()).getOrElse("status", "").asInstanceOf[String]
    status should be("SUBMITTED")
  }

  "JobAPIService" should "not submit the permanently failed/max attempts reached request while doing retry" in {

  }

  it should "return response for get data request" in {
    val response = JobAPIService.getDataRequest("dev-portal", "14621312DB7F8ED99BA1B16D8B430FAC")
  }

  it should "return the list of jobs in descending order" in {

    DBUtil.cluster.connect("local_platform_db").execute("DELETE FROM local_platform_db.job_request WHERE client_key='partner1'")
    val request_data1 = """{"filter":{"start_date":"2016-11-19","end_date":"2016-11-20","tags":["becb887fe82f24c644482eb30041da6d88bd8150"]}}"""
    val request_data2 = """{"filter":{"start_date":"2016-11-19","end_date":"2016-11-20","tags":["test-tag"],"events":["OE_ASSESS"]}}"""

    val requests = Array(
      JobRequest(Option("partner1"), Option("1234"), None, Option("SUBMITTED"), Option(request_data1),
        Option(1), Option(DateTime.now()), None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
      JobRequest(Option("partner1"), Option("273645"), Option("test-job-id"), Option("COMPLETED"), Option(request_data2),
        Option(1), Option(DateTime.parse("2017-01-08", CommonUtil.dateFormat)), Option("https://test-location"), Option(DateTime.parse("2017-01-08", CommonUtil.dateFormat)), None, None, None, None, None, Option(123234), Option(532), Option(12343453L), None, None, None, None, None))

    DBUtil.saveJobRequest(requests)

    val res = JobAPIService.getDataRequestList("partner1", 10)
    val resultMap = res.result.get
    val jobRes = JSONUtils.deserialize[List[JobResponse]](JSONUtils.serialize(resultMap.get("jobs").get))
    jobRes.length should be(2)

    // fetch data with limit less than the number of record available
    val res2 = JobAPIService.getDataRequestList("partner1", 1)
    val resultMap2 = res2.result.get
    val jobRes2 = JSONUtils.deserialize[List[JobResponse]](JSONUtils.serialize(resultMap2.get("jobs").get))
    jobRes2.length should be(1)

    // trying to fetch the record with a key for which data is not available
    val res1 = JobAPIService.getDataRequestList("testKey", 10)
    val resultMap1 = res1.result.get.asInstanceOf[Map[String, AnyRef]]
    resultMap1.get("count").get.asInstanceOf[Int] should be(0)
  }

  "JobAPIService" should "return different request id for same data having different client keys" in {
    val request1 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"output_format": "json", "dataset_id": "eks-consumption-raw", "filter":{"start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"], "app_id": "Ekstep", "channel": "KAR"}}}"""
    val response1 = JobAPIService.dataRequest(request1, "in.ekstep")
    val request2 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-test"},"request":{"output_format": "json", "dataset_id": "eks-consumption-raw", "filter":{"start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"], "app_id": "Ekstep", "channel": "KAR"}}}"""
    val response2 = JobAPIService.dataRequest(request2, "in.ekstep")
    response2.result.head.get("request_id").get should not be (response1.result.head.get("request_id").get)

  }

  // Channel Exhaust Test Cases
  // -ve Test cases
  it should "return a CLIENT_ERROR in the response if we set `datasetID` other than these ('raw', 'summary', 'metrics', 'failed')" in {
    val datasetId = "test"
    val resObj = JobAPIService.getChannelData("in.ekstep", datasetId, "2018-05-14", "2018-05-15", None)
    resObj.responseCode should be("CLIENT_ERROR")
    resObj.params.errmsg should be("Please provide 'eventType' value should be one of these -> ('raw' or 'summary' or 'metrics', or 'failed') in your request URL")
  }

  it should "return a CLIENT_ERROR in the response if 'fromDate' is empty" in {
    val fromDate = ""
    val resObj = JobAPIService.getChannelData("in.ekstep", "raw", fromDate, "2018-05-15", None)
    resObj.responseCode should be("CLIENT_ERROR")
    resObj.params.errmsg should be("Please provide 'from' in query string")
  }

  it should "return a CLIENT_ERROR in the response if 'endDate' is empty older than fromDate" in {
    val toDate = "2018-05-10"
    val resObj = JobAPIService.getChannelData("in.ekstep", "raw", "2018-05-15", toDate, None)
    resObj.responseCode should be("CLIENT_ERROR")
    resObj.params.errmsg should be("Date range should not be -ve. Please check your 'from' & 'to'")
  }

  it should "return a CLIENT_ERROR in the response if 'endDate' is a future date" in {
    val toDate = new LocalDate().plusDays(1).toString()
    val resObj = JobAPIService.getChannelData("in.ekstep", "raw", "2018-05-15", toDate, None)
    resObj.responseCode should be("CLIENT_ERROR")
    resObj.params.errmsg should be("'to' should be LESSER OR EQUAL TO today's date..")
  }

  it should "return a CLIENT_ERROR in the response if date_range > 10" in {
    val toDate = new LocalDate().toString()
    val fromDate = new LocalDate().minusDays(11).toString()

    val resObj = JobAPIService.getChannelData("in.ekstep", "raw", fromDate, toDate, None)
    resObj.responseCode should be("CLIENT_ERROR")
    resObj.params.errmsg should be("Date range should be < 10 days")
  }

  // +ve test cases

  ignore should "return a successfull response if 'to' is empty" in {
    val toDate = ""
    val resObj = JobAPIService.getChannelData("in.ekstep", "raw", "2018-05-20", toDate, None)
    resObj.responseCode should be("OK")
  }

  ignore should "return a successfull response if datasetID is one of these ('raw', 'summary', 'metrics', 'failed') - S3" in {
    val datasetId = "raw"
    val resObj = JobAPIService.getChannelData("in.ekstep", datasetId, "2018-05-20", "2018-05-21", None)
    resObj.responseCode should be("OK")
  }

  ignore should "return a successfull response if we pass code=ds" in {
    val resObj = JobAPIService.getChannelData("in.ekstep", "raw", "2018-05-20", "2018-05-20", Option("device-summary"))
    resObj.responseCode should be("OK")
    val res = resObj.result.getOrElse(Map())
    res.contains("telemetryURLs") should be(true)
  }
}
