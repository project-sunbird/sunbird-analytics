package org.ekstep.analytics.api.service

import org.ekstep.analytics.api._
import org.ekstep.analytics.api.util.IDBUtil
import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.pattern.ask
import akka.util.Timeout
import org.joda.time.DateTime
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class TestExperimentAPIService extends BaseSpec with MockitoSugar {

    override def beforeAll() {
        super.beforeAll();
    }

    override def afterAll() {
        super.afterAll();
    }

    "ExperimentAPIService" should  "return response for data request" in {
        val mockDBUtil: IDBUtil = mock[IDBUtil]

        implicit val timeout = Timeout(20 second)
        implicit def executionContext = scala.concurrent.ExecutionContext.global
        val request = """{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"U1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"organisations.orgName":["sunbird"]}},"data":{"startDate":"2021-08-09","endDate":"2021-08-21","key":"/org/profile","client":"portal"}}}"""
        implicit val actorSystem = ActorSystem("testActorSystem", config)
        val experimentActor = TestActorRef(new ExperimentAPIService(mockDBUtil))
        when(mockDBUtil.getExperiementDefinition("U1234")).thenReturn(Option(new ExperimentDefinition("U1234","USER_ORG","Experiment to get users to explore page","User1","Admin1",Option(new DateTime()),Option(new DateTime()) , "{\"type\":\"user\",\"filters\":{\"organisations.orgName\":[\"sunbird\"]}}","{\"startDate\":\"2019-09-10\",\"endDate\":\"2019-12-12\",\"key\":\"/org/profile\",\"client\":\"portal\"}",Option("ACTIVE"), Option("Experiment Mapped Sucessfully"), Option(Map("userMatched" -> 10000)))))
        val response = experimentActor ? CreateExperimentRequest(request,config)
        val futureResponse: Future[Response] = response.mapTo[Response]


        futureResponse map {
            case value: Response => {
                value.responseCode should be ("OK")
            }
        }
    }

    "ExperimentAPIService" should "return error response for data request" in {
        val mockDBUtil: IDBUtil = mock[IDBUtil]
        implicit val timeout = Timeout(20 second)
        implicit def executionContext = scala.concurrent.ExecutionContext.global
        val request = """{"id":"ekstep.analytics.dataset.request.submit","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"organisations.orgName":["sunbird"]}},"data":{"startDate":"2021-08-01","endDate":"2021-08-02","key":"/org/profile","client":"portal"}}}"""
        implicit val actorSystem = ActorSystem("testActorSystem", config)
        val experimentActor = TestActorRef(new ExperimentAPIService(mockDBUtil))
        when(mockDBUtil.getExperiementDefinition("U1234")).thenReturn(Option(new ExperimentDefinition("U1234","USER_ORG","Experiment to get users to explore page","User1","Admin1",Option(new DateTime()),Option(new DateTime()) , "{\"type\":\"user\",\"filters\":{\"organisations.orgName\":[\"sunbird\"]}}","{\"startDate\":\"2019-09-10\",\"endDate\":\"2019-12-12\",\"key\":\"/org/profile\",\"client\":\"portal\"}",Option("ACTIVE"), Option("Experiment Mapped Sucessfully"), Option(Map("userMatched" -> 10000)))))
        val response = experimentActor ? CreateExperimentRequest(request,config)
        val responseFuture: Future[Response] = response.mapTo[Response]
        responseFuture map {
            case value: Response => {
                value.responseCode should be ("CLIENT_ERROR")
            }
        }
    }

    "ExperimentAPIService" should "return the experiment for experimentid" in {
        val mockDBUtil: IDBUtil = mock[IDBUtil]
        implicit val timeout = Timeout(20 second)
        implicit def executionContext = scala.concurrent.ExecutionContext.global
        implicit val actorSystem = ActorSystem("testActorSystem", config)
        val experimentActor = TestActorRef(new ExperimentAPIService(mockDBUtil))
        when(mockDBUtil.getExperiementDefinition("U1234")).thenReturn(Option(new ExperimentDefinition("U1234","USER_ORG","Experiment to get users to explore page","User1","Admin1",Option(new DateTime()),Option(new DateTime()) , "{\"type\":\"user\",\"filters\":{\"organisations.orgName\":[\"sunbird\"]}}","{\"startDate\":\"2019-09-10\",\"endDate\":\"2019-12-12\",\"key\":\"/org/profile\",\"client\":\"portal\"}",Option("ACTIVE"), Option("Experiment Mapped Sucessfully"), Option(Map("userMatched" -> 10000)))))
        val response = experimentActor ? GetExperimentRequest("U1234", config)
        val futureResponse: Future[Response] = response.mapTo[Response]

        futureResponse map {
            case value: Response => {
                  value.responseCode should be ("OK")
            }
        }
    }

    "ExperimentAPIService" should "contain \"no experiemnt available with the given experimentid\"" in {
        val mockDBUtil: IDBUtil = mock[IDBUtil]
        implicit val timeout = Timeout(20 second)
        implicit def executionContext = scala.concurrent.ExecutionContext.global
        implicit val actorSystem = ActorSystem("testActorSystem", config)
        val experimentActor = TestActorRef(new ExperimentAPIService(mockDBUtil))
        when(mockDBUtil.getExperiementDefinition("U1235")).thenReturn(Option(new ExperimentDefinition("U1234","USER_ORG","Experiment to get users to explore page","User1","Admin1",Option(new DateTime()),Option(new DateTime()) , "{\"type\":\"user\",\"filters\":{\"organisations.orgName\":[\"sunbird\"]}}","{\"startDate\":\"2019-09-10\",\"endDate\":\"2019-12-12\",\"key\":\"/org/profile\",\"client\":\"portal\"}",Option("ACTIVE"), Option("Experiment Mapped Sucessfully"), Option(Map("userMatched" -> 10000)))))
        val response = experimentActor ? GetExperimentRequest("U1235", config)
        val futureResponse: Future[Response] = response.mapTo[Response]

        futureResponse map {
            case value: Response => {
                value.params.errmsg should be ("no experiemnt available with the given experimentid")
            }
        }
    }

}