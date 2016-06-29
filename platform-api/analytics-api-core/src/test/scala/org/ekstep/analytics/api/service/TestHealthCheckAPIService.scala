package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.SparkSpec
import org.ekstep.analytics.api.util.JSONUtils
import org.ekstep.analytics.api.Response
import org.joda.time.DateTimeUtils
import org.ekstep.analytics.api.ContentUsageSummaryFact
import com.datastax.spark.connector._
import org.ekstep.analytics.api.Constants
import com.datastax.spark.connector.cql.CassandraConnector

class TestHealthCheckAPIService extends SparkSpec {
    
    override def beforeAll() {
        super.beforeAll();
    }
    
    override def afterAll() {
        super.afterAll();
    }
    
    "HealthCheckAPIService" should "return health statusof APIs" in {
        val response = HealthCheckAPIService.getHealthStatus
        val resp = JSONUtils.deserialize[Response](response)
        
        resp.id should be ("ekstep.analytics-api.health");
        resp.params.status should be ("successful");
        
        val result = resp.result.get;
        result.get("name").get should be ("analytics-platform-api")
        result.get("checks").get.asInstanceOf[List[AnyRef]].length should be (2)
    }
}