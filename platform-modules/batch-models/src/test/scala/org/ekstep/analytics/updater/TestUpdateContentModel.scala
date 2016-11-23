package org.ekstep.analytics.updater

import org.scalatest._
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework._
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.CommonUtil
import org.joda.time.DateTime
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.util.ContentUsageSummaryFact
import org.ekstep.analytics.util.ContentPopularitySummaryFact
import org.ekstep.analytics.adapter.ContentAdapter
import org.ekstep.analytics.framework.util.RestUtil

/**
 * @author Santhosh
 */
class TestUpdateContentModel extends SparkSpec(null) {

    override def beforeAll() {
        super.beforeAll()

        val usageSummaries = Array(ContentUsageSummaryFact(0, "org.ekstep.delta", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, null),
            ContentUsageSummaryFact(0, "numeracy_374", "all", DateTime.now, DateTime.now, DateTime.now, 220.5, 4, 52.5, 76, 23.56, 15, 3.14, null));
        sc.parallelize(usageSummaries).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT);

        val popularitySummary = Array(ContentPopularitySummaryFact(0, "org.ekstep.delta", "all", 22, 53, List(("Test comment1", DateTime.now),("Test comment", DateTime.now)), List((3, DateTime.now),(4, DateTime.now), (3, DateTime.now)), 3.33),
            ContentPopularitySummaryFact(0, "org.ekstep.vayuthewind", "all", 22, 53, List(("Test comment1", DateTime.now),("Test comment", DateTime.now)), List((3, DateTime.now),(4, DateTime.now), (3, DateTime.now)), 3.33))
        sc.parallelize(popularitySummary).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT);
    }

    override def afterAll() {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("DELETE FROM " + Constants.CONTENT_KEY_SPACE_NAME +"." + Constants.CONTENT_USAGE_SUMMARY_FACT + " where d_period = 0 and d_tag = 'all' and d_content_id ='org.ekstep.delta'");
            session.execute("DELETE FROM " + Constants.CONTENT_KEY_SPACE_NAME +"." + Constants.CONTENT_USAGE_SUMMARY_FACT + " where d_period = 0 and d_tag = 'all' and d_content_id ='numeracy_374'");
            session.execute("DELETE FROM " + Constants.CONTENT_KEY_SPACE_NAME +"." + Constants.CONTENT_POPULARITY_SUMMARY_FACT + " where d_period = 0 and d_tag = 'all' and d_content_id ='org.ekstep.delta'");
            session.execute("DELETE FROM " + Constants.CONTENT_KEY_SPACE_NAME +"." + Constants.CONTENT_POPULARITY_SUMMARY_FACT + " where d_period = 0 and d_tag = 'all' and d_content_id ='org.ekstep.vayuthewind'");
        }
        super.afterAll();
    }

    "UpdateContentPopularityDB" should "populate content usage and popularity metrics in content model" in {

        val rdd = DataFetcher.fetchBatchData[DerivedEvent](Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/content-popularity/test-data.json"))))));
        val rdd2 = UpdateContentModel.execute(rdd, Option(Map()));
        var out = rdd2.collect();
        out.length should be(2);

        val resp = RestUtil.get[ContentResponse](Constants.getContent("org.ekstep.delta") + "?fields=popularity,me_totalSessionsCount,me_totalTimespent,me_totalInteractions,me_averageInteractionsPerMin,me_averageSessionsPerDevice,me_totalDevices,me_averageTimespentPerSession,me_averageRating,me_totalDownloads,me_totalSideloads,me_totalRatings,me_totalComments")
        resp.result.content.get("identifier").get should be ("org.ekstep.delta");
        resp.result.content.get("popularity").get should be (450.0);
        resp.result.content.get("me_totalSessionsCount").get should be (4);
        resp.result.content.get("me_totalTimespent").get should be (450.0);
        resp.result.content.get("me_totalInteractions").get should be (100);
        resp.result.content.get("me_averageInteractionsPerMin").get should be (23.56);
        resp.result.content.get("me_averageSessionsPerDevice").get should be (2.15);
        resp.result.content.get("me_totalDevices").get should be (11);
        resp.result.content.get("me_averageTimespentPerSession").get should be (112.5);
        
        resp.result.content.get("me_averageRating").get should be (3.33);
        resp.result.content.get("me_totalDownloads").get should be (22);
        resp.result.content.get("me_totalSideloads").get should be (53);
        resp.result.content.get("me_totalRatings").get should be (3);
        resp.result.content.get("me_totalComments").get should be (2);
    }
    
    it should "populate content usage metrics when popularity metrics are blank in content model" in {

        val rdd = DataFetcher.fetchBatchData[DerivedEvent](Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/content-popularity/test-data-2.json"))))));
        val rdd2 = UpdateContentModel.execute(rdd, Option(Map()));
        var out = rdd2.collect();
        out.length should be(1);

        val resp = RestUtil.get[ContentResponse](Constants.getContent("numeracy_374") + "?fields=popularity,me_totalSessionsCount,me_totalTimespent,me_totalInteractions,me_averageInteractionsPerMin,me_averageSessionsPerDevice,me_totalDevices,me_averageTimespentPerSession,me_averageRating,me_totalDownloads,me_totalSideloads,me_totalRatings,me_totalComments")
        resp.result.content.get("identifier").get should be ("numeracy_374");
        resp.result.content.get("popularity").get should be (220.5);
        resp.result.content.get("me_totalSessionsCount").get should be (4);
        resp.result.content.get("me_totalTimespent").get should be (220.5);
        resp.result.content.get("me_totalInteractions").get should be (76);
        resp.result.content.get("me_averageInteractionsPerMin").get should be (23.56);
        resp.result.content.get("me_averageSessionsPerDevice").get should be (3.14);
        resp.result.content.get("me_totalDevices").get should be (15);
        resp.result.content.get("me_averageTimespentPerSession").get should be (52.5);
        
        resp.result.content.get("me_averageRating").isDefined should be (false);
        resp.result.content.get("me_totalDownloads").isDefined should be (false);
        resp.result.content.get("me_totalSideloads").isDefined should be (false);
        resp.result.content.get("me_totalRatings").isDefined should be (false);
        resp.result.content.get("me_totalComments").isDefined should be (false);
    }
}
case class ContentResult(content: Map[String, AnyRef]); 
case class ContentResponse(id: String, result: ContentResult);