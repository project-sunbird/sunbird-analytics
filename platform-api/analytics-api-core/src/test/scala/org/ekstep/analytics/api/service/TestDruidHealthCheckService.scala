package org.ekstep.analytics.api.service

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.HTTPClient
import org.mockito.Mockito._


class TestDruidHealthCheckAPIService extends BaseSpec {
  override def beforeAll() {
    super.beforeAll();
  }

  override def afterAll() {
    super.afterAll();
  }

  "DruidHealthCheckService" should "return health status of druid datasources" in {

    val HTTPClientMock = mock[HTTPClient]
    implicit val actorSystem = ActorSystem("testActorSystem", config)

    val apiURL = AppConf.getConfig("druid.coordinator.host") + AppConf.getConfig("druid.healthcheck.url")
    when(HTTPClientMock.get[Map[String, Double]](apiURL)).thenReturn(Map("summary-events" -> 100.0))

    val healthCheckActor = TestActorRef(new DruidHealthCheckService(HTTPClientMock)).underlyingActor
    healthCheckActor.getStatus should be("http_druid_health_check_status{datasource=\"summary-events\"} 100.0\n")
  }
}
