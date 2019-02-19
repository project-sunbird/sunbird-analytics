package org.ekstep.analytics.updater

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util._
import org.joda.time.DateTime

/**
 * @author Santhosh
 */
class TestUpdateContentModel extends SparkSpec(null) {

    "UpdateContentModel" should "populate content usage and popularity metrics in content model" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE local_platform_db.workflow_usage_summary_fact")
            session.execute("TRUNCATE local_content_db.content_popularity_summary_fact")

        }

        val workflowUsageSummaries = Array(WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), AppConf.getConfig("default.app.id"), "all", "Textbook", "mode1", "all", "org.ekstep.delta", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1),Array(2),Array(3), Some("Textbook")),
            WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), AppConf.getConfig("default.app.id"), "all", "Worksheet", "mode1", "all", "org.ekstep.vayuthewind", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1),Array(2),Array(3), Some("Worksheet")))
        sc.parallelize(workflowUsageSummaries).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT);

        val popularitySummary = Array(ContentPopularitySummaryFact2(0, "org.ekstep.delta", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33),
            ContentPopularitySummaryFact2(0, "org.ekstep.vayuthewind", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33))
        sc.parallelize(popularitySummary).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT);

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
        dataMap.get("me_averageTimespentPerSession").get.get("nv").get should be(112.5)

        dataMap.get("me_averageRating").get.get("nv").get should be(3.33)
        dataMap.get("me_totalDownloads").get.get("nv").get should be(22)
        dataMap.get("me_totalSideloads").get.get("nv").get should be(53)
        dataMap.get("me_totalRatings").get.get("nv").get should be(3)
        dataMap.get("me_totalComments").get.get("nv").get should be(2)

        data1.audit should be(false)

    }

    it should "populate content usage metrics when popularity metrics are blank in content model and vice-versa" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE local_platform_db.workflow_usage_summary_fact")
            session.execute("TRUNCATE local_content_db.content_popularity_summary_fact")
        }

        val workflowUsageSummaries = Array(WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), AppConf.getConfig("default.app.id"), "all", "Textbook", "mode1", "all", "org.ekstep.delta", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1),Array(2),Array(3), Some("Textbook")),
            WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), AppConf.getConfig("default.app.id"), "all", "Worksheet", "mode1", "all", "numeracy_374", "all", DateTime.now, DateTime.now, DateTime.now, 220.5, 4, 52.5, 76, 23.56, 11, 2.15, 12, 15, 18, Array(1),Array(2),Array(3), Some("Worksheet")))
        sc.parallelize(workflowUsageSummaries).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT);

        val popularitySummary = Array(ContentPopularitySummaryFact2(0, "org.ekstep.delta", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33, Option(DateTime.now().minusDays(2))),
            ContentPopularitySummaryFact2(0, "org.ekstep.vayuthewind", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33))
        sc.parallelize(popularitySummary).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT);

        val rdd = UpdateContentModel.execute(sc.emptyRDD, Option(Map()));
        val out = rdd.collect();
        out.length should be(3);
        
        val data1 = out.filter { x => "numeracy_374".equals(x.nodeUniqueId) }.head
        data1.nodeUniqueId should be("numeracy_374")
        val dataMap1 = data1.transactionData.get("properties").get
        //consumption data
        dataMap1.get("me_totalSessionsCount").get.get("nv").get should be(4)
        dataMap1.get("me_totalTimespent").get.get("nv").get should be(220.5)
        dataMap1.get("me_totalInteractions").get.get("nv").get should be(76)
        dataMap1.get("me_averageInteractionsPerMin").get.get("nv").get should be(23.56)
        dataMap1.get("me_averageTimespentPerSession").get.get("nv").get should be(52.5)

        dataMap1.get("me_averageRating").isDefined should be(false)
        dataMap1.get("me_totalDownloads").isDefined should be(false)
        dataMap1.get("me_totalSideloads").isDefined should be(false)
        dataMap1.get("me_totalRatings").isDefined should be(false)
        dataMap1.get("me_totalComments").isDefined should be(false)

        data1.audit should be(false)

        val data2 = out.filter { x => "org.ekstep.vayuthewind".equals(x.nodeUniqueId) }.head
        data2.nodeUniqueId should be("org.ekstep.vayuthewind")
        val dataMap2 = data2.transactionData.get("properties").get
        //consumption data
        dataMap2.get("me_totalSessionsCount").isDefined should be(false)
        dataMap2.get("me_totalTimespent").isDefined should be(false)
        dataMap2.get("me_totalInteractions").isDefined should be(false)
        dataMap2.get("me_averageInteractionsPerMin").isDefined should be(false)
        dataMap2.get("me_averageTimespentPerSession").isDefined should be(false)
        dataMap2.get("me_averageRating").get.get("nv").get should be(3.33)
        dataMap2.get("me_totalDownloads").get.get("nv").get should be(22)
        dataMap2.get("me_totalSideloads").get.get("nv").get should be(53)
        dataMap2.get("me_totalRatings").get.get("nv").get should be(3)
        dataMap2.get("me_totalComments").get.get("nv").get should be(2)

        data2.audit should be(false)
    }
    
    it should "return zero output when no records found for given date" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE local_platform_db.workflow_usage_summary_fact")
            session.execute("TRUNCATE local_content_db.content_popularity_summary_fact")
        }

        val workflowUsageSummaries = Array(WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), AppConf.getConfig("default.app.id"), "all", "Textbook", "mode1", "dId-1", "org.ekstep.delta", "userId-1", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1),Array(2),Array(3), Some("Textbook"), Option(DateTime.now().minusDays(2))),
            WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), AppConf.getConfig("default.app.id"), "all", "Worksheet", "mode1", "dId-2", "numeracy_374", "userId-2", DateTime.now, DateTime.now, DateTime.now, 220.5, 4, 52.5, 76, 23.56, 11, 2.15, 12, 15, 18, Array(1),Array(2),Array(3), Some("Worksheet"), Option(DateTime.now().minusDays(2))));
        sc.parallelize(workflowUsageSummaries).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT);

        val popularitySummary = Array(ContentPopularitySummaryFact2(0, "org.ekstep.delta", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33, Option(DateTime.now().minusDays(2))),
            ContentPopularitySummaryFact2(0, "org.ekstep.vayuthewind", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33, Option(DateTime.now().minusDays(2))))
        sc.parallelize(popularitySummary).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT);

        val rdd = UpdateContentModel.execute(sc.emptyRDD, Option(Map()));
        val out = rdd.collect();
        out.length should be(0);
    }

    it should "not include workflow summary records having content_id 'all'" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE local_platform_db.workflow_usage_summary_fact")
            session.execute("TRUNCATE local_content_db.content_popularity_summary_fact")
        }
        val workflowUsageSummaries = Array(WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), AppConf.getConfig("default.app.id"), "all", "Textbook", "mode1", "all", "all", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1),Array(2),Array(3), Some("Textbook")),
            WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), AppConf.getConfig("default.app.id"), "all", "Worksheet", "mode1", "all", "numeracy_374", "all", DateTime.now, DateTime.now, DateTime.now, 220.5, 4, 52.5, 76, 23.56, 11, 2.15, 12, 15, 18, Array(1),Array(2),Array(3), Some("Worksheet")))
        sc.parallelize(workflowUsageSummaries).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT);

        val rdd = UpdateContentModel.execute(sc.emptyRDD, Option(Map()));
        val out = rdd.collect();
        out.length should be(1);

    }

    it should "update the objectType if value of contentType present in the corresponding List values " in {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE local_platform_db.workflow_usage_summary_fact")
            session.execute("TRUNCATE local_content_db.content_popularity_summary_fact")
            val workflowUsageSummaries = Array(WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), AppConf.getConfig("default.app.id"), "all", "Textbook", "mode1", "all", "org.ekstep.delta", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1),Array(2),Array(3), Some("Textbook")),
              WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), AppConf.getConfig("default.app.id"), "all", "Worksheet", "mode1", "all", "org.ekstep.vayuthewind", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1),Array(2),Array(3), Some("Worksheet")),
              WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), AppConf.getConfig("default.app.id"), "all", "Worksheet", "mode1", "all", "numeracy_374", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1),Array(2),Array(3), Some("WorkflowItem")))
            sc.parallelize(workflowUsageSummaries).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT)

            val popularitySummary = Array(ContentPopularitySummaryFact2(0, "org.ekstep.delta", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33),
                ContentPopularitySummaryFact2(0, "org.ekstep.vayuthewind", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33))
            sc.parallelize(popularitySummary).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT)
        }
        val rdd = UpdateContentModel.execute(sc.emptyRDD, Option(Map("contentTypes" -> Map("Content" -> List("Resource",
            "Collection",
            "TextBook",
            "LessonPlan",
            "Course",
            "Template",
            "Asset",
            "Plugin",
            "LessonPlanUnit",
            "CourseUnit",
            "TextBookUnit"), "Framework" -> List("WorkSheet")))))
        rdd.collect().foreach{ x =>
          if(x.nodeUniqueId.equalsIgnoreCase("org.ekstep.vayuthewind")){
              x.objectType should be("Framework")
          }else if(x.nodeUniqueId.equalsIgnoreCase("org.ekstep.delta"))
              x.objectType should be("Content")
          else x.objectType should be("WorkflowItem")

        }

    }
}