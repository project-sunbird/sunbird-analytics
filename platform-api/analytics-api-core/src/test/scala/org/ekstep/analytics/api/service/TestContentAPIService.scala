package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.SparkSpec
import org.ekstep.analytics.api.util.JSONUtils
import org.ekstep.analytics.api.Response
import org.joda.time.DateTimeUtils
import org.ekstep.analytics.api.ContentUsageSummaryFact
import com.datastax.spark.connector._
import org.ekstep.analytics.api.Constants
import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._

class TestContentAPIService extends SparkSpec {
    
    override def beforeAll() {
        super.beforeAll()
        DateTimeUtils.setCurrentMillisFixed(1464859204280L); // Fix the date-time to be returned by DateTime.now()
        // Load test data
        val rdd = loadFile[ContentUsageSummaryFact]("src/test/resources/content-summaries/test_content_summaries.log");
        rdd.saveToCassandra(Constants.CONTENT_DB, Constants.CONTENT_SUMMARY_FACT_TABLE);
        println("Content saved...");
    }
    
    override def afterAll() {
        // Cleanup test data
//        CassandraConnector(sc.getConf).withSessionDo { session =>
//            val query = "DELETE FROM " + Constants.CONTENT_DB + "." + Constants.CONTENT_SUMMARY_FACT_TABLE + " where d_content_id='org.ekstep.test123'"
//            session.execute(query);
//        }
        super.afterAll();
    }
    
    ignore should "enrich content and create content vectors" in {
        val config = ConfigFactory.parseMap(Map("python.home" -> "",
        			"content2vec_scripts_path" -> "src/test/resources/python/main/vidyavaani").asJava)
        				.withFallback(ConfigFactory.load());
        val resp = ContentAPIService.contentToVec("domain_66036")(sc, config)
        println("### Response ###", resp);
       		
    }
  
}