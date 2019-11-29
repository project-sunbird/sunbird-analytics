package org.ekstep.analytics.job

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.model.SparkSpec
import org.scalamock.scalatest.MockFactory
import org.ekstep.analytics.job.report.BaseReportsJob
import org.ekstep.analytics.job.report.StateAdminReportJob
import org.ekstep.analytics.job.report.ShadowUserData

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
    spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    shadowUserDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .schema(shadowUserEncoder)
      .load("src/test/resources/state-admin-report-updater/shadowUserTable.csv")
      .cache()

    orgDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/test/resources/state-admin-report-updater/orgTable.csv")
      .cache()

  }


  it should "generate reports" in {
    (reporterMock.loadData _).expects(spark, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace) , Some(shadowUserEncoder))
      .returning(shadowUserDF)
    (reporterMock.loadData _).expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace), None)
      .returning(orgDF)
    val reportDF = StateAdminReportJob.generateReport()(spark)

    // There are only 2 state information in the test csv
    assert(reportDF.count == 2)
  }
}