package org.ekstep.analytics.updater

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util._
import org.joda.time.DateTime

/**
  * @author
  */
class TestUpdateDashboardtModel extends SparkSpec(null) {

  "UpdateDashboardModel" should "Should find the unique device count" in {

//    CassandraConnector(sc.getConf).withSessionDo { session =>
//      session.execute("TRUNCATE local_platform_db.workflow_usage_summary_fact")
//    }
//    val updateDashboardModel = Array(
//      WorkFlowUsageSummaryFact(0, "b00bc992ef25f1a9a8d63291e20efc8d", "prod.diksha.app", "all", "content", "play", "874ed8a5-782e-4f6c-8f36-e0288455901e", "org.ekstep.delta", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Textbook")),
//      WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), "prod.diksha.portal", "all", "content", "play", "874ed8a5-782e-4f6c-8f36-e0288455901e", "org.ekstep.vayuthewind", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Worksheet")),
//      WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), "prod.diksha.portal", "all", "Worksheet", "mode1", "874ed8a5-782e-4f6c-8f36-e0288455901e", "org.ekstep.ek", "all", DateTime.now, DateTime.now, DateTime.now, 40, 4, 112.5, 100, 23.56, 11, 33, 12, 15, 18, Array(1), Array(2), Array(3), Some("Worksheet")),
//      WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), "prod.diksha.portal", "all", "Worksheet", "mode1", "5743895-53457439-54389638-59834758-53", "org.ekstep.vayuthewind", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Worksheet"))
//    )


    //sc.parallelize(updateDashboardModel).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT)
    val rdd = UpdateDashboardModel.execute(sc.emptyRDD, Option(Map("date" -> new DateTime().toString(CommonUtil.dateFormat).asInstanceOf[AnyRef])))
    val out = rdd.collect()
    println(out.head)
    println(JSONUtils.serialize(out.head))


  }

  //  it should "populate content usage metrics when popularity metrics are blank in content model and vice-versa" in {
  //
  //    CassandraConnector(sc.getConf).withSessionDo { session =>
  //      session.execute("TRUNCATE local_platform_db.workflow_usage_summary_fact")
  //      session.execute("TRUNCATE local_content_db.content_popularity_summary_fact")
  //    }
  //
  //    val workflowUsageSummaries = Array(WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), AppConf.getConfig("default.app.id"), "all", "Textbook", "mode1", "all", "org.ekstep.delta", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Textbook")),
  //      WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), AppConf.getConfig("default.app.id"), "all", "Worksheet", "mode1", "all", "numeracy_374", "all", DateTime.now, DateTime.now, DateTime.now, 220.5, 4, 52.5, 76, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Worksheet")))
  //    sc.parallelize(workflowUsageSummaries).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT);
  //
  //    val popularitySummary = Array(ContentPopularitySummaryFact2(0, "org.ekstep.delta", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33, Option(DateTime.now().minusDays(2))),
  //      ContentPopularitySummaryFact2(0, "org.ekstep.vayuthewind", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33))
  //    sc.parallelize(popularitySummary).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT);
  //
  //    val rdd = UpdateContentModel.execute(sc.emptyRDD, Option(Map()));
  //    val out = rdd.collect();
  //    out.length should be(3);
  //
  //    val data1 = out.filter { x => "numeracy_374".equals(x.nodeUniqueId) }.head
  //    data1.nodeUniqueId should be("numeracy_374")
  //    val dataMap1 = data1.transactionData.get("properties").get
  //    //consumption data
  //    dataMap1.get("me_totalSessionsCount").get.get("nv").get should be(4)
  //    dataMap1.get("me_totalTimespent").get.get("nv").get should be(220.5)
  //    dataMap1.get("me_totalInteractions").get.get("nv").get should be(76)
  //    dataMap1.get("me_averageInteractionsPerMin").get.get("nv").get should be(23.56)
  //    dataMap1.get("me_averageTimespentPerSession").get.get("nv").get should be(52.5)
  //
  //    dataMap1.get("me_averageRating").isDefined should be(false)
  //    dataMap1.get("me_totalDownloads").isDefined should be(false)
  //    dataMap1.get("me_totalSideloads").isDefined should be(false)
  //    dataMap1.get("me_totalRatings").isDefined should be(false)
  //    dataMap1.get("me_totalComments").isDefined should be(false)
  //
  //    val data2 = out.filter { x => "org.ekstep.vayuthewind".equals(x.nodeUniqueId) }.head
  //    data2.nodeUniqueId should be("org.ekstep.vayuthewind")
  //    val dataMap2 = data2.transactionData.get("properties").get
  //    //consumption data
  //    dataMap2.get("me_totalSessionsCount").isDefined should be(false)
  //    dataMap2.get("me_totalTimespent").isDefined should be(false)
  //    dataMap2.get("me_totalInteractions").isDefined should be(false)
  //    dataMap2.get("me_averageInteractionsPerMin").isDefined should be(false)
  //    dataMap2.get("me_averageTimespentPerSession").isDefined should be(false)
  //    dataMap2.get("me_averageRating").get.get("nv").get should be(3.33)
  //    dataMap2.get("me_totalDownloads").get.get("nv").get should be(22)
  //    dataMap2.get("me_totalSideloads").get.get("nv").get should be(53)
  //    dataMap2.get("me_totalRatings").get.get("nv").get should be(3)
  //    dataMap2.get("me_totalComments").get.get("nv").get should be(2)
  //
  //  }
  //
  //  it should "return zero output when no records found for given date" in {
  //
  //    CassandraConnector(sc.getConf).withSessionDo { session =>
  //      session.execute("TRUNCATE local_platform_db.workflow_usage_summary_fact")
  //      session.execute("TRUNCATE local_content_db.content_popularity_summary_fact")
  //    }
  //
  //    val workflowUsageSummaries = Array(WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), AppConf.getConfig("default.app.id"), "all", "Textbook", "mode1", "dId-1", "org.ekstep.delta", "userId-1", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Textbook"), Option(DateTime.now().minusDays(2))),
  //      WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), AppConf.getConfig("default.app.id"), "all", "Worksheet", "mode1", "dId-2", "numeracy_374", "userId-2", DateTime.now, DateTime.now, DateTime.now, 220.5, 4, 52.5, 76, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Worksheet"), Option(DateTime.now().minusDays(2))));
  //    sc.parallelize(workflowUsageSummaries).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT);
  //
  //    val popularitySummary = Array(ContentPopularitySummaryFact2(0, "org.ekstep.delta", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33, Option(DateTime.now().minusDays(2))),
  //      ContentPopularitySummaryFact2(0, "org.ekstep.vayuthewind", "all", AppConf.getConfig("default.app.id"), AppConf.getConfig("default.channel.id"), 22, 53, List(("Test comment1", DateTime.now.getMillis), ("Test comment", DateTime.now.getMillis)), List((3, DateTime.now.getMillis), (4, DateTime.now.getMillis), (3, DateTime.now.getMillis)), 3.33, Option(DateTime.now().minusDays(2))))
  //    sc.parallelize(popularitySummary).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT);
  //
  //    val rdd = UpdateContentModel.execute(sc.emptyRDD, Option(Map()));
  //    val out = rdd.collect();
  //    out.length should be(0);
  //  }
  //
  //  it should "not include workflow summary records having content_id 'all'" in {
  //
  //    CassandraConnector(sc.getConf).withSessionDo { session =>
  //      session.execute("TRUNCATE local_platform_db.workflow_usage_summary_fact")
  //      session.execute("TRUNCATE local_content_db.content_popularity_summary_fact")
  //    }
  //    val workflowUsageSummaries = Array(WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), AppConf.getConfig("default.app.id"), "all", "Textbook", "mode1", "all", "all", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Textbook")),
  //      WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), AppConf.getConfig("default.app.id"), "all", "Worksheet", "mode1", "all", "numeracy_374", "all", DateTime.now, DateTime.now, DateTime.now, 220.5, 4, 52.5, 76, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Worksheet")))
  //    sc.parallelize(workflowUsageSummaries).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT);
  //
  //    val rdd = UpdateContentModel.execute(sc.emptyRDD, Option(Map()));
  //    val out = rdd.collect();
  //    out.length should be(1);
  //
  //  }
}