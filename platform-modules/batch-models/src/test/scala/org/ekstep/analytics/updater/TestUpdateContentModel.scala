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
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.util.ContentPopularitySummaryFact2
import org.ekstep.analytics.framework.util.JSONUtils

/**
 * @author Santhosh
 */
class TestUpdateContentModel extends SparkSpec(null) {

    "UpdateContentModel" should "populate content usage and popularity metrics in content model" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE local_content_db.content_usage_summary_fact");
            session.execute("TRUNCATE local_content_db.content_popularity_summary_fact");
            session.execute("TRUNCATE local_creation_metrics_db.ce_usage_summary_fact");
            session.execute("TRUNCATE local_creation_metrics_db.content_creation_metrics_fact");
        }
        val usageSummaries = Array(ContentUsageSummaryFact(0, "org.ekstep.delta", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, null),
            ContentUsageSummaryFact(0, "numeracy_374", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), DateTime.now, DateTime.now, DateTime.now, 220.5, 4, 52.5, 76, 23.56, 15, 3.14, null, Option(DateTime.now().minusDays(2))));
        sc.parallelize(usageSummaries).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT);

        val popularitySummary = Array(ContentPopularitySummaryFact2(0, "org.ekstep.delta", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33),
            ContentPopularitySummaryFact2(0, "org.ekstep.vayuthewind", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33))
        sc.parallelize(popularitySummary).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT);
        
        val creationSummary = Array(CEUsageSummaryFact(0, "org.ekstep.delta", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 2, 6, 48.0, 8.0, DateTime.now().getMillis),
                CEUsageSummaryFact(0, "org.ekstep.vayuthewind", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 2, 6, 48.0, 8.0, DateTime.now().getMillis))
        sc.parallelize(creationSummary).saveToCassandra(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY);
        
        val creationMetrics = Array(ContentCreationMetrics("org.ekstep.delta", 1, 0, 12, 6, 3, Map(), Option(48.0), Option(8.0), Option("Draft"), Option(DateTime.now().getMillis), 1, DateTime.now().getMillis),
                ContentCreationMetrics("org.ekstep.vayuthewind", 1, 0, 12, 6, 3, Map(), Option(48.0), Option(8.0), Option("Draft"), Option(DateTime.now().getMillis), 1, DateTime.now().getMillis))
        sc.parallelize(creationMetrics).saveToCassandra(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_CREATION_TABLE);
        
        val rdd = UpdateContentModel.execute(sc.emptyRDD, Option(Map("date" -> new DateTime().toString(CommonUtil.dateFormat).asInstanceOf[AnyRef])));
        val out = rdd.collect();
        out.length should be(2);
        
        val data1 = out.filter { x => "org.ekstep.delta".equals(x.nodeUniqueId) }.head
        data1.nodeUniqueId should be("org.ekstep.delta")
        val dataMap = data1.transactionData.get("properties").get
        //consumption data
        dataMap.get("me_totalSessionsCount").get.get("nv").get should be(4)
        dataMap.get("me_totalTimespent").get.get("nv").get should be(450.0)
        dataMap.get("me_totalInteractions").get.get("nv").get should be(100)
        dataMap.get("me_averageInteractionsPerMin").get.get("nv").get should be(23.56)
        dataMap.get("me_averageSessionsPerDevice").get.get("nv").get should be(2.15)
        dataMap.get("me_totalDevices").get.get("nv").get should be(11)
        dataMap.get("me_averageTimespentPerSession").get.get("nv").get should be(112.5)
        dataMap.get("me_averageRating").get.get("nv").get should be(3.33)
        dataMap.get("me_totalDownloads").get.get("nv").get should be(22)
        dataMap.get("me_totalSideloads").get.get("nv").get should be(53)
        dataMap.get("me_totalRatings").get.get("nv").get should be(3)
        dataMap.get("me_totalComments").get.get("nv").get should be(2)
        // creation data
        dataMap.get("me_creationTimespent").get.get("nv").get should be(48.0)
        dataMap.get("me_creationSessions").get.get("nv").get should be(6)
        dataMap.get("me_avgCreationTsPerSession").get.get("nv").get should be(8.0)
        dataMap.get("me_imagesCount").get.get("nv").get should be(12)
        dataMap.get("me_audiosCount").get.get("nv").get should be(6)
        dataMap.get("me_videosCount").get.get("nv").get should be(3)
        dataMap.get("me_timespentDraft").get.get("nv").get should be(48.0)
        dataMap.get("me_timespentReview").get.get("nv").get should be(8.0)
    }

    it should "populate content usage metrics when popularity metrics are blank in content model and vice-versa" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE local_content_db.content_usage_summary_fact");
            session.execute("TRUNCATE local_content_db.content_popularity_summary_fact");
            session.execute("TRUNCATE local_creation_metrics_db.ce_usage_summary_fact");
            session.execute("TRUNCATE local_creation_metrics_db.content_creation_metrics_fact");
        }
        val usageSummaries = Array(ContentUsageSummaryFact(0, "org.ekstep.delta", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, null, Option(DateTime.now().minusDays(2))),
            ContentUsageSummaryFact(0, "numeracy_374", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), DateTime.now, DateTime.now, DateTime.now, 220.5, 4, 52.5, 76, 23.56, 15, 3.14, null));
        sc.parallelize(usageSummaries).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT);

        val popularitySummary = Array(ContentPopularitySummaryFact2(0, "org.ekstep.delta", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33, Option(DateTime.now().minusDays(2))),
            ContentPopularitySummaryFact2(0, "org.ekstep.vayuthewind", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33))
        sc.parallelize(popularitySummary).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT);
        
        val creationSummary = Array(CEUsageSummaryFact(0, "org.ekstep.delta", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 2, 6, 48.0, 8.0, DateTime.now().minusDays(2).getMillis),
                CEUsageSummaryFact(0, "org.ekstep.vayuthewind", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 2, 6, 48.0, 8.0, DateTime.now().getMillis),
                CEUsageSummaryFact(0, "numeracy_374", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 2, 6, 48.0, 8.0, DateTime.now().minusDays(2).getMillis))
        sc.parallelize(creationSummary).saveToCassandra(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY);
        
        val creationMetrics = Array(ContentCreationMetrics("org.ekstep.delta", 1, 0, 12, 6, 3, Map(), Option(48.0), Option(8.0), Option("Draft"), Option(DateTime.now().getMillis), 1, DateTime.now().minusDays(2).getMillis),
                ContentCreationMetrics("org.ekstep.vayuthewind", 1, 0, 12, 6, 3, Map(), Option(48.0), Option(8.0), Option("Draft"), Option(DateTime.now().getMillis), 1, DateTime.now().minusDays(2).getMillis),
                ContentCreationMetrics("numeracy_374", 1, 0, 12, 6, 3, Map(), Option(48.0), Option(8.0), Option("Draft"), Option(DateTime.now().getMillis), 1, DateTime.now().getMillis))
        sc.parallelize(creationMetrics).saveToCassandra(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_CREATION_TABLE);
        
        val rdd = UpdateContentModel.execute(sc.emptyRDD, Option(Map()));
        val out = rdd.collect();
        out.length should be(2);
        
        val data1 = out.filter { x => "numeracy_374".equals(x.nodeUniqueId) }.head
        data1.nodeUniqueId should be("numeracy_374")
        val dataMap1 = data1.transactionData.get("properties").get
        //consumption data
        dataMap1.get("me_totalSessionsCount").get.get("nv").get should be(4)
        dataMap1.get("me_totalTimespent").get.get("nv").get should be(220.5)
        dataMap1.get("me_totalInteractions").get.get("nv").get should be(76)
        dataMap1.get("me_averageInteractionsPerMin").get.get("nv").get should be(23.56)
        dataMap1.get("me_averageSessionsPerDevice").get.get("nv").get should be(3.14)
        dataMap1.get("me_totalDevices").get.get("nv").get should be(15)
        dataMap1.get("me_averageTimespentPerSession").get.get("nv").get should be(52.5)
        dataMap1.get("me_averageRating").isDefined should be(false)
        dataMap1.get("me_totalDownloads").isDefined should be(false)
        dataMap1.get("me_totalSideloads").isDefined should be(false)
        dataMap1.get("me_totalRatings").isDefined should be(false)
        dataMap1.get("me_totalComments").isDefined should be(false)
        // creation data
        dataMap1.get("me_creationTimespent").isDefined should be(false)
        dataMap1.get("me_creationSessions").isDefined should be(false)
        dataMap1.get("me_avgCreationTsPerSession").isDefined should be(false)
        dataMap1.get("me_imagesCount").get.get("nv").get should be(12)
        dataMap1.get("me_audiosCount").get.get("nv").get should be(6)
        dataMap1.get("me_videosCount").get.get("nv").get should be(3)
        dataMap1.get("me_timespentDraft").get.get("nv").get should be(48.0)
        dataMap1.get("me_timespentReview").get.get("nv").get should be(8.0)
        
        val data2 = out.filter { x => "org.ekstep.vayuthewind".equals(x.nodeUniqueId) }.head
        data2.nodeUniqueId should be("org.ekstep.vayuthewind")
        val dataMap2 = data2.transactionData.get("properties").get
        //consumption data
        dataMap2.get("me_totalSessionsCount").isDefined should be(false)
        dataMap2.get("me_totalTimespent").isDefined should be(false)
        dataMap2.get("me_totalInteractions").isDefined should be(false)
        dataMap2.get("me_averageInteractionsPerMin").isDefined should be(false)
        dataMap2.get("me_averageSessionsPerDevice").isDefined should be(false)
        dataMap2.get("me_totalDevices").isDefined should be(false)
        dataMap2.get("me_averageTimespentPerSession").isDefined should be(false)
        dataMap2.get("me_averageRating").get.get("nv").get should be(3.33)
        dataMap2.get("me_totalDownloads").get.get("nv").get should be(22)
        dataMap2.get("me_totalSideloads").get.get("nv").get should be(53)
        dataMap2.get("me_totalRatings").get.get("nv").get should be(3)
        dataMap2.get("me_totalComments").get.get("nv").get should be(2)
        // creation data
        dataMap2.get("me_creationTimespent").get.get("nv").get should be(48.0)
        dataMap2.get("me_creationSessions").get.get("nv").get should be(6)
        dataMap2.get("me_avgCreationTsPerSession").get.get("nv").get should be(8.0)
        dataMap2.get("me_imagesCount").isDefined should be(false)
        dataMap2.get("me_audiosCount").isDefined should be(false)
        dataMap2.get("me_videosCount").isDefined should be(false)
        dataMap2.get("me_timespentDraft").isDefined should be(false)
        dataMap2.get("me_timespentReview").isDefined should be(false)
    }
    
    it should "return zero output when no records found for given date" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE local_content_db.content_usage_summary_fact");
            session.execute("TRUNCATE local_content_db.content_popularity_summary_fact");
            session.execute("TRUNCATE local_creation_metrics_db.ce_usage_summary_fact");
            session.execute("TRUNCATE local_creation_metrics_db.content_creation_metrics_fact");
            
        }
        val usageSummaries = Array(ContentUsageSummaryFact(0, "org.ekstep.delta", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, null, Option(DateTime.now().minusDays(2))),
            ContentUsageSummaryFact(0, "numeracy_374", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), DateTime.now, DateTime.now, DateTime.now, 220.5, 4, 52.5, 76, 23.56, 15, 3.14, null, Option(DateTime.now().minusDays(2))));
        sc.parallelize(usageSummaries).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT);

        val popularitySummary = Array(ContentPopularitySummaryFact2(0, "org.ekstep.delta", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33, Option(DateTime.now().minusDays(2))),
            ContentPopularitySummaryFact2(0, "org.ekstep.vayuthewind", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33, Option(DateTime.now().minusDays(2))))
        sc.parallelize(popularitySummary).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT);
        
        val creationSummary = Array(CEUsageSummaryFact(0, "org.ekstep.delta", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 2, 6, 48.0, 8.0, DateTime.now().minusDays(2).getMillis),
                CEUsageSummaryFact(0, "org.ekstep.vayuthewind", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 2, 6, 48.0, 8.0, DateTime.now().minusDays(2).getMillis))
        sc.parallelize(creationSummary).saveToCassandra(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY);
        
        val creationMetrics = Array(ContentCreationMetrics("org.ekstep.delta", 1, 0, 12, 6, 3, Map(), Option(48.0), Option(8.0), Option("Draft"), Option(DateTime.now().getMillis), 1, DateTime.now().minusDays(2).getMillis),
                ContentCreationMetrics("org.ekstep.vayuthewind", 1, 0, 12, 6, 3, Map(), Option(48.0), Option(8.0), Option("Draft"), Option(DateTime.now().getMillis), 1, DateTime.now().minusDays(2).getMillis))
        sc.parallelize(creationMetrics).saveToCassandra(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_CREATION_TABLE);
        
        val rdd = UpdateContentModel.execute(sc.emptyRDD, Option(Map()));
        val out = rdd.collect();
        out.length should be(0);
    }
}