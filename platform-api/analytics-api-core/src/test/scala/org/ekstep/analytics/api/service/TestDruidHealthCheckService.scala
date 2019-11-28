package org.ekstep.analytics.api.service

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.HTTPClient
import org.mockito.Mockito._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._


class TestDruidHealthCheckAPIService extends BaseSpec {
  implicit val timeout: Timeout = 20 seconds

  override def beforeAll() {
    super.beforeAll();
  }

  override def afterAll() {
    super.afterAll();
  }

  "DruidHealthCheckService" should "return health status of druid datasources" in {

    val HTTPClientMock = mock[HTTPClient]
    implicit val actorSystem = ActorSystem("testActorSystem", config)
    implicit val executor =  scala.concurrent.ExecutionContext.global

    val apiURL = AppConf.getConfig("druid.coordinator.host") + AppConf.getConfig("druid.healthcheck.url")
    when(HTTPClientMock.get[Map[String, Double]](apiURL)).thenReturn(Map("summary-events" -> 100.0))

    val healthCheckActorRef = TestActorRef(new DruidHealthCheckService(HTTPClientMock))
    val response = healthCheckActorRef ? "health"
    response.map{ data =>
      data should be("http_druid_health_check_status{datasource=\"summary-events\"} 100.0\n")
    }

    when(HTTPClientMock.get[Map[String, Double]](apiURL)).thenThrow(new RuntimeException("something went wrong here!"))
    val response2 = healthCheckActorRef ? "health"
    response2.map{ data =>
      data should be("")
    }
  }
}
