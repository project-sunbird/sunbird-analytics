package org.ekstep.analytics.api.service

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import org.ekstep.analytics.api.{BaseSpec, Response}
import org.ekstep.analytics.api.util.JSONUtils
import org.ekstep.analytics.api.util.RedisUtil

class TestHealthCheckAPIService extends BaseSpec {

    private implicit val system: ActorSystem = ActorSystem("health-check-test-actor-system", config)
    val redisUtil = mock[RedisUtil]

    override def beforeAll() {
        super.beforeAll();
    }
    
    override def afterAll() {
        super.afterAll();
    }

    
    "HealthCheckAPIService" should "return health statusof APIs" in {
        val response = HealthCheckAPIService.getHealthStatus()
        val resp = JSONUtils.deserialize[Response](response)
        
        resp.id should be ("ekstep.analytics-api.health");
        resp.params.status should be ("successful");
        
        val result = resp.result.get;
        result.get("name").get should be ("analytics-platform-api")
        result.get("checks").get.asInstanceOf[List[AnyRef]].length should be (5)
    }
}