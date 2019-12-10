package org.ekstep.analytics.job

import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.job.report.{BaseReportSpec, BaseReportsJob, ShadowUserData, StateAdminReportJob}
import org.ekstep.analytics.util.{EmbeddedCassandra}
import org.scalamock.scalatest.MockFactory

class TestStateAdminReportJob extends BaseReportSpec with MockFactory {

  implicit var spark: SparkSession = _
  var map: Map[String, String] = _
  var shadowUserDF: DataFrame = _
  var orgDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdKeyspace = "sunbird"
  val shadowUserEncoder = Encoders.product[ShadowUserData].schema

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    EmbeddedCassandra.loadData("src/test/resources/reports/reports_test_data.cql") // Load test data in embedded cassandra server
  }


  "StateAdminReportJob" should "generate reports" in {
    val reportDF = StateAdminReportJob.generateReport()(spark)
    assert(reportDF.columns.contains("index") === true)
    assert(reportDF.columns.contains("registered") === true)
    assert(reportDF.columns.contains("blocks") === true)
    assert(reportDF.columns.contains("schools") === true)
    assert(reportDF.columns.contains("districtName") === true)
    assert(reportDF.columns.contains("slug") === true)
    val apslug = reportDF.where(col("slug") === "ApSlug")
    val districtName = apslug.select("districtName").collect().map(_ (0)).toList
    assert(districtName(0) === "GULBARGA")
  }

}