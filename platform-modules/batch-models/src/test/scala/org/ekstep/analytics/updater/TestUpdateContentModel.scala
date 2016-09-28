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

        val popularitySummary = ContentPopularitySummaryFact(0, "org.ekstep.delta", "all", 22, 53, List(("Test comment1", DateTime.now),("Test comment", DateTime.now)), List((3, DateTime.now),(4, DateTime.now), (3, DateTime.now)), 3.33)
        sc.parallelize(Seq(popularitySummary)).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT);
    }

    override def afterAll() {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("DELETE FROM " + Constants.CONTENT_KEY_SPACE_NAME +"." + Constants.CONTENT_USAGE_SUMMARY_FACT + " where d_period = 0 and d_tag = 'all' and d_content_id ='org.ekstep.delta'");
            session.execute("DELETE FROM " + Constants.CONTENT_KEY_SPACE_NAME +"." + Constants.CONTENT_USAGE_SUMMARY_FACT + " where d_period = 0 and d_tag = 'all' and d_content_id ='numeracy_374'");
            session.execute("DELETE FROM " + Constants.CONTENT_KEY_SPACE_NAME +"." + Constants.CONTENT_POPULARITY_SUMMARY_FACT + " where d_period = 0 and d_tag = 'all' and d_content_id ='org.ekstep.delta'");
        }
        super.afterAll();
    }

    "UpdateContentPopularityDB" should "populate content usage and popularity metrics in content model" in {

        val rdd = DataFetcher.fetchBatchData[DerivedEvent](Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/content-popularity/test-data.json"))))));
        val rdd2 = UpdateContentModel.execute(rdd, Option(Map()));
        var out = rdd2.collect();
        out.length should be(1);

        val resp = RestUtil.get[ContentResponse](Constants.getContent("org.ekstep.delta") + "?fields=popularity,me:totalSessionsCount,me:totalTimespent,me:totalInteractions,me:averageInteractionsPerMin,me:averageSessionsPerDevice,me:totalDevices,me:averageTimespentPerSession,me:averageRating,me:totalDownloads,me:totalSideloads,me:totalRatings,me:totalComments")
        resp.result.content.get("identifier").get should be ("org.ekstep.delta");
        resp.result.content.get("popularity").get should be (450.0);
        resp.result.content.get("me:totalSessionsCount").get should be (4);
        resp.result.content.get("me:totalTimespent").get should be (450.0);
        resp.result.content.get("me:totalInteractions").get should be (100);
        resp.result.content.get("me:averageInteractionsPerMin").get should be (23.56);
        resp.result.content.get("me:averageSessionsPerDevice").get should be (2.15);
        resp.result.content.get("me:totalDevices").get should be (11);
        resp.result.content.get("me:averageTimespentPerSession").get should be (112.5);
        
        resp.result.content.get("me:averageRating").get should be (3.33);
        resp.result.content.get("me:totalDownloads").get should be (22);
        resp.result.content.get("me:totalSideloads").get should be (53);
        resp.result.content.get("me:totalRatings").get should be (3);
        resp.result.content.get("me:totalComments").get should be (2);
    }
    
    it should "populate content usage metrics when popularity metrics are blank in content model" in {

        val rdd = DataFetcher.fetchBatchData[DerivedEvent](Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/content-popularity/test-data-2.json"))))));
        val rdd2 = UpdateContentModel.execute(rdd, Option(Map()));
        var out = rdd2.collect();
        out.length should be(1);

        val resp = RestUtil.get[ContentResponse](Constants.getContent("numeracy_374") + "?fields=popularity,me:totalSessionsCount,me:totalTimespent,me:totalInteractions,me:averageInteractionsPerMin,me:averageSessionsPerDevice,me:totalDevices,me:averageTimespentPerSession,me:averageRating,me:totalDownloads,me:totalSideloads,me:totalRatings,me:totalComments")
        resp.result.content.get("identifier").get should be ("numeracy_374");
        resp.result.content.get("popularity").get should be (220.5);
        resp.result.content.get("me:totalSessionsCount").get should be (4);
        resp.result.content.get("me:totalTimespent").get should be (220.5);
        resp.result.content.get("me:totalInteractions").get should be (76);
        resp.result.content.get("me:averageInteractionsPerMin").get should be (23.56);
        resp.result.content.get("me:averageSessionsPerDevice").get should be (3.14);
        resp.result.content.get("me:totalDevices").get should be (15);
        resp.result.content.get("me:averageTimespentPerSession").get should be (52.5);
        
        resp.result.content.get("me:averageRating").isDefined should be (false);
        resp.result.content.get("me:totalDownloads").isDefined should be (false);
        resp.result.content.get("me:totalSideloads").isDefined should be (false);
        resp.result.content.get("me:totalRatings").isDefined should be (false);
        resp.result.content.get("me:totalComments").isDefined should be (false);
    }
}
case class ContentResult(content: Map[String, AnyRef]); 
case class ContentResponse(id: String, result: ContentResult);