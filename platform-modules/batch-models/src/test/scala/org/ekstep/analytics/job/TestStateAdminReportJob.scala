package org.ekstep.analytics.job

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.model.SparkSpec
import org.mockito.Mockito
import org.neo4j.kernel.impl.store.UnderlyingStorageException
import org.scalamock.scalatest.MockFactory

import scala.collection.Map


class TestStateAdminReportJob extends SparkSpec(null) with MockFactory {
  var spark: SparkSession = _
  var shadowUserDF: DataFrame = _
  var locationDF: DataFrame = _
  var orgDF: DataFrame = _
  var reporterMock: ReportGenerator = mock[ReportGenerator]
  val sunbirdKeyspace = "sunbird"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    shadowUserDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/state-admin-report-updater/shadowUserTable.csv")
      .cache()

    locationDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/state-admin-report-updater/locationTable.csv")
      .cache()

    orgDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/test/resources/state-admin-report-updater/orgTable.csv")
      .cache()
  }


  "TestUpdateStateAdminReport" should "generate reports" in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace))
      .returning(shadowUserDF).repeat(3)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF).repeat(Range(0, 10))

    val reportDF = StateAdminReportJob
      .prepareReport(spark, reporterMock.loadData)
      .cache()

    // There are only 2 state information in the test csv
    assert(reportDF.count == 2)
  }
}