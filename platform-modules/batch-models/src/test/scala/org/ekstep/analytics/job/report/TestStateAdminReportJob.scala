package org.ekstep.analytics.job

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.model.SparkSpec
import org.scalamock.scalatest.MockFactory
import org.ekstep.analytics.job.report.BaseReportsJob
import org.ekstep.analytics.job.report.StateAdminReportJob
import org.ekstep.analytics.job.report.ShadowUserData
import org.ekstep.analytics.util.EmbeddedCassandra

class TestStateAdminReportJob extends SparkSpec(null) with MockFactory {

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
    EmbeddedCassandra.loadData("../../platform-modules/batch-models/src/test/resources/reports/reports_test_data.cql"); // Load test data in embedded cassandra server
  }


  "StateAdminReportJob" should "generate reports" in {
    val reportDF = StateAdminReportJob.generateReport()(spark)
    assert(reportDF.select("School id").count() == 6)
  }
}