package org.ekstep.analytics.updater

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.{WorkFlowUsageSummaryFact, _}
import org.joda.time.DateTime


/**
  * @author Manjunath Davanam <manjunathd@ilimi.in>
  */
case class TestPortalMetrics(noOfUniqueDevices: Long, totalContentPlayTime: Double, totalTimeSpent: Double, totalContentPublished: Long)

class TestUpdatePortalMetrics extends SparkSpec(null) {

  /**
    * Truncate the data from the database before run the testcase
    */
  private def cleanDataBase(): Unit ={
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute("TRUNCATE local_platform_db.workflow_usage_summary_fact")
    }
  }

  /**
    * Invoke this method to save the data into cassandra database
    *
    */
  private def saveToDB(data:Array[WorkFlowUsageSummaryFact]): Unit ={
    sc.parallelize(data).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT)
  }

  /**
    * Which is used to execute updateDashboard data product
    */
  private def executeDataProduct():RDD[PortalMetrics]={
    UpdatePortalMetrics.execute(sc.emptyRDD, Option(Map("date" -> new DateTime().toString(CommonUtil.dateFormat).asInstanceOf[AnyRef])))
  }

  "UpdateDashboardModel" should "Should find the unique device count,totalContentPublished,totalTimeSpent,totalContentPlayTime and should filter when d_time>0(Cumulative)" in {
    cleanDataBase()
    val inputData = Array(
      WorkFlowUsageSummaryFact(0, "b00bc992ef25f1a9a8d63291e20efc8d", "prod.diksha.app", "all", "content", "play", "874ed8a5-782e-4f6c-8f36-e0288455901e", "org.ekstep.delta", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Textbook")),
      WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), "prod.diksha.portal", "all", "content", "play", "874ed8a5-782e-4f6c-8f36-e0288455901e", "org.ekstep.vayuthewind", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Worksheet")),
      WorkFlowUsageSummaryFact(22, AppConf.getConfig("default.channel.id"), "prod.diksha.portal", "all", "Worksheet", "mode1", "874ed8a5-782e-4f6c-8f36-e0288455901e", "org.ekstep.ek", "all", DateTime.now, DateTime.now, DateTime.now, 40, 4, 112.5, 100, 23.56, 11, 33, 12, 15, 18, Array(1), Array(2), Array(3), Some("Worksheet")),
      WorkFlowUsageSummaryFact(0, AppConf.getConfig("default.channel.id"), "prod.diksha.portal", "all", "Worksheet", "mode1", "5743895-53457439-54389638-59834758-53", "org.ekstep.vayuthewind", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Worksheet"))
    )
    saveToDB(inputData)
    val rdd = executeDataProduct()
    val out = rdd.collect()
    println(JSONUtils.serialize(out.head))
    val dashboardSummary = JSONUtils.deserialize[TestPortalMetrics](JSONUtils.serialize(out.head.metrics_summary))
    dashboardSummary.totalContentPublished should be(749)
    dashboardSummary.noOfUniqueDevices should be(2)
    dashboardSummary.totalTimeSpent should be(0.0)
    dashboardSummary.totalContentPlayTime should be(0.0)
  }

  it should "populate zero records when no data is found in Database" in {
    println("second test case")
    cleanDataBase()
    saveToDB(Array())
    val result = executeDataProduct().collect().head
    val dashboardSummary = JSONUtils.deserialize[TestPortalMetrics](JSONUtils.serialize(result.metrics_summary))
    dashboardSummary.totalContentPublished should be(749)
    dashboardSummary.noOfUniqueDevices should be(0)
    dashboardSummary.totalTimeSpent should be(0)
    dashboardSummary.totalContentPlayTime should be(0)
    println(JSONUtils.serialize(result))
  }

  it should "Populate the accurate device count excluding when deviceId='all', totalTimeSpent and totalContentPlaySession " in{
    println("3rd testcase")
    cleanDataBase()
    val inputData = Array(
      WorkFlowUsageSummaryFact(0, "b00bc992ef25f1a9a8d63291e20efc8d", "prod.diksha.app", "all", "content", "play", "874ed8a5-782e-4f6c-8f36-e0288455901e", "org.ekstep.delta", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 100, 100, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Textbook")),
      WorkFlowUsageSummaryFact(0, "b00bc992ef25f1a9a8d63291e20efc8d", "prod.diksha.app", "all", "content", "play", "78349678-782e-4f6c-8f36-e02884559085", "org.ekstep.delta", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 30, 10, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Textbook")),
      WorkFlowUsageSummaryFact(0, "b00bc992ef25f1a9a8d63291e20efc8d", "prod.diksha.app", "all", "content", "play", "534557346543-782e-4f6c-8f36-e02884559085", "org.ekstep.delta", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 0, 20.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Textbook")),
      WorkFlowUsageSummaryFact(0, "b00bc992ef25f1a9a8d63291e20efc8d", "prod.diksha.app", "all", "content", "play", "all", "org.ekstep.delta", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 10, 20, Array(1), Array(2), Array(3), Some("Textbook")),
      WorkFlowUsageSummaryFact(0, "b00bc992ef25f1a9a8d63291e20efc8df","prod.diksha.app", "all", "content", "play", "all", "org.ekstep.delta", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 4, 112.5, 100, 23.56, 11, 2.15, 12, 20, 600, Array(1), Array(2), Array(3), Some("Textbook")),
      WorkFlowUsageSummaryFact(0, "b00bc992ef25f1a9a8d63291e20efc8d", "prod.diksha.app", "all", "content", "edit", "534557346543-782e-4f6c-8f36-e02884559085", "org.ekstep.delta", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 30, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Textbook")),
      WorkFlowUsageSummaryFact(0, "b00bc992ef25f1a9a8d63291e20efc8da", "prod.diksha.app", "all", "app", "edit", "534557346543-782e-4f6c-8f36-e02884559085a", "org.ekstep.delta", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 30, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Textbook")),
      WorkFlowUsageSummaryFact(0, "b00bc992ef25f1a9a8d63291e20efc8db", "prod.diksha.app", "all", "app", "edit", "534557346543-782e-4f6c-8f36-e02884559085b", "org.ekstep.delta", "all", DateTime.now, DateTime.now, DateTime.now, 450.0, 30, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Textbook")),
      WorkFlowUsageSummaryFact(0, "b00bc992ef25f1a9a8d63291e20efc8dc", "prod.diksha.app", "all", "session", "edit", "534557346543-782e-4f6c-8f36-e02884559085c", "org.ekstep.delta", "all", DateTime.now, DateTime.now, DateTime.now, 100.0, 30, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Textbook")),
      WorkFlowUsageSummaryFact(0, "b00bc992ef25f1a9a8d63291e20efc8dd", "prod.diksha.app", "all", "session", "edit", "534557346543-782e-4f6c-8f36-e02884559085d", "org.ekstep.delta", "all", DateTime.now, DateTime.now, DateTime.now, 430.8, 30, 112.5, 100, 23.56, 11, 2.15, 12, 15, 18, Array(1), Array(2), Array(3), Some("Textbook"))
    )
    saveToDB(inputData)
    val result = executeDataProduct().collect().head
    val dashboardSummary = JSONUtils.deserialize[TestPortalMetrics](JSONUtils.serialize(result.metrics_summary))
    dashboardSummary.totalContentPublished should be(749)
    dashboardSummary.noOfUniqueDevices should be(7)
    dashboardSummary.totalTimeSpent should be(0.4)
    dashboardSummary.totalContentPlayTime should be(0.04)
    println(JSONUtils.serialize(result))
  }

}