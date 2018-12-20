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
case class TestPortalMetrics(noOfUniqueDevices: Long, totalContentPlaySessions: Double, totalTimeSpent: Double, totalContentPublished: Long)

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
    val rdd = executeDataProduct()
    val out = rdd.collect()
    println(JSONUtils.serialize(out.head))
    val dashboardSummary = JSONUtils.deserialize[TestPortalMetrics](JSONUtils.serialize(out.head.metrics_summary))
    dashboardSummary.totalContentPublished should be(0)
    dashboardSummary.noOfUniqueDevices should be(3)
    dashboardSummary.totalTimeSpent should be(108.9)
    dashboardSummary.totalContentPlaySessions should be(624)
  }

}