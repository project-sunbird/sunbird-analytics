package org.ekstep.analytics.job

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.model.SparkSpec
import org.scalamock.scalatest.MockFactory
import org.ekstep.analytics.job.report.StateAdminGeoReportJob
import org.ekstep.analytics.job.report.BaseReportsJob
import org.ekstep.analytics.util.EmbeddedCassandra

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
    EmbeddedCassandra.loadData("../../platform-modules/batch-models/src/test/resources/reports/reports_test_data.cql"); // Load test data in embedded cassandra server
  }

  "StateAdminGeoReportJob" should "generate reports" in {
    val reportDF = StateAdminGeoReportJob.generateGeoReport()(spark)
    assert(reportDF.count() === 6)
    assert(reportDF.select("District id").distinct().count == 4)
  }
}