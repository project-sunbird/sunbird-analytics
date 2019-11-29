package org.ekstep.analytics.job

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.model.SparkSpec
import org.scalamock.scalatest.MockFactory
import org.ekstep.analytics.job.report.StateAdminGeoReportJob
import org.ekstep.analytics.job.report.BaseReportsJob

class TestStateAdminGeoReportJob extends SparkSpec(null) with MockFactory {

  implicit var spark: SparkSession = _
  var map: Map[String, String] = _
  var shadowUserDF: DataFrame = _
  var orgDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdKeyspace = "sunbird"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    orgDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/test/resources/state-admin-report-updater/orgTable.csv")
      .cache()

  }

  it should "generate reports" in {
    (reporterMock.loadData _).expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace), None)
      .returning(orgDF)
    val reportDF = StateAdminGeoReportJob.generateGeoSummaryReport()(spark)

    // There are only 5 sub-orgs in the test csv
    assert(reportDF.count == 5)
  }
}