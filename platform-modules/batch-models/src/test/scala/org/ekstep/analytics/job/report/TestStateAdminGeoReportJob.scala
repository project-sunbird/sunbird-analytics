package org.ekstep.analytics.job

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.model.SparkSpec
import org.scalamock.scalatest.MockFactory
import org.ekstep.analytics.job.report.StateAdminGeoReportJob
import org.ekstep.analytics.job.report.BaseReportsJob
import org.ekstep.analytics.util.EmbeddedCassandra
import org.sunbird.cloud.storage.conf.AppConf

class TestStateAdminGeoReportJob extends SparkSpec(null) with MockFactory {

  implicit var spark: SparkSession = _
  var map: Map[String, String] = _
  var shadowUserDF: DataFrame = _
  var orgDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdKeyspace = "sunbird"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession()
    EmbeddedCassandra.loadData("src/test/resources/reports/reports_test_data.cql") // Load test data in embedded cassandra server
  }

  "StateAdminGeoReportJob" should "generate reports" in {
    val reportDF = StateAdminGeoReportJob.generateGeoReport()(spark)
    assert(reportDF.count() === 6)
    assert(reportDF.select("District id").distinct().count == 4)
  }

  "StateAdminGeoReportJob" should "able to saveGeoDetailsReport" in {
    val tempDir = AppConf.getConfig("admin.metrics.temp.dir")
    val df = spark.createDataFrame(Seq(
      ("school_123", "NVPHS", "564378567534", "true", "location_123", "tumkur", "bengaluru", ""),
      ("school_222", "SSIT", "564578348", "true", "location_123", "tumkur", "bengaluru", "")
    )).toDF("id", "orgname", "channel", "status", "locid", "name", "parentid", "type")

    StateAdminGeoReportJob.saveGeoDetailsReport(df, tempDir)
  }

  "StateAdminGeoReportJob" should "able to saveSummaryReport" in {
    val tempDir = AppConf.getConfig("admin.metrics.temp.dir")
    val df = spark.createDataFrame(Seq(
      ("0", "0", "1", "1", "0", "1", "1", "org.sunbird"),
      ("1", "0", "1", "3", "0", "1", "1", "54735743")
    )).toDF("accounts_unclaimed", "accounts_validated",
      "accounts_rejected", "FAILED", "MULTIMATCH", "ORGEXTIDMISMATCH", "accounts_failed", "channel")
    StateAdminGeoReportJob.saveSummaryReport(df, tempDir)
  }
}