package org.ekstep.analytics.api.service

import org.ekstep.analytics.api._

class TestExperimentAPIService extends BaseSpec {

    "ExperimentAPIService" should "return response for data request" in {
        val request = """{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"U1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"organisations.orgName":["sunbird"]}},"data":{"startDate":"2021-08-09","endDate":"2021-08-21","key":"/org/profile","client":"portal"}}}"""
        val response = ExperimentAPIService.createRequest(request)
        response.responseCode should be("OK")
    }

    "ExperimentAPIService" should "return error response for data request" in {
        val request = """{"id":"ekstep.analytics.dataset.request.submit","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"organisations.orgName":["sunbird"]}},"data":{"startDate":"2021-08-01","endDate":"2021-08-02","key":"/org/profile","client":"portal"}}}"""
        val response = ExperimentAPIService.createRequest(request)
        response.responseCode should be("CLIENT_ERROR")
    }

    "ExperimentAPIService" should "return the experiment for experimentid" in {
        val request = """{"id":"ekstep.analytics.dataset.request.submit","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"organisations.orgName":["sunbird"]}},"data":{"startDate":"2022-08-01","endDate":"2022-08-02","key":"/org/profile","client":"portal"}}}"""
        val response = ExperimentAPIService.getExperimentDefinition("U1234")
        response.responseCode should be("OK")
    }

    "ExperimentAPIService" should "return the error for no experimentid" in {
        val response = ExperimentAPIService.getExperimentDefinition("H1234")
        response.params.errmsg  should be ("no experiemnt available with the given experimentid")

    }

}