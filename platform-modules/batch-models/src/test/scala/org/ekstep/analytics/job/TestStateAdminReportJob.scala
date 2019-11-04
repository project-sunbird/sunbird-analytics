package org.ekstep.analytics.job

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.model.SparkSpec
import org.mockito.Mockito
import org.mockito.Mockito._
import org.neo4j.kernel.impl.store.UnderlyingStorageException
import org.scalamock.scalatest.MockFactory
import org.scalatest.mockito.MockitoSugar
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.Map


class TestStateAdminReportJob extends SparkSpec(null) with MockitoSugar {

  var spark: SparkSession = _
  var shadowUserDF: DataFrame = _
  var locationDF: DataFrame = _
  var orgDF: DataFrame = _
  var reporterMock: ReportGenerator = mock[ReportGenerator](Mockito.withSettings().serializable())
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

    when(reporterMock.loadData(spark, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace)))
      .thenReturn(shadowUserDF, shadowUserDF, shadowUserDF)
      .thenThrow(new RuntimeException("Called more than 3 timess"))
    when(reporterMock.loadData(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))).thenReturn(locationDF)
    when(reporterMock.loadData(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))).thenReturn(orgDF)
  }


  "TestUpdateStateAdminReport" should "generate reports" in {

    // (reporterMock.loadData _).expects(spark, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace)).returning(shadowUserDF).repeat(3)

    // when(reporterMock.loadData(spark, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace))).thenReturn(shadowUserDF, shadowUserDF, shadowUserDF)
    // when(reporterMock.loadData(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))).thenReturn(locationDF)
    // when(reporterMock.loadData(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))).thenReturn(orgDF)

    // (reporterMock.loadData _).expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace)).returning(locationDF)

    // (reporterMock.loadData _).expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace)).returning(orgDF).repeat(Range(0, 10))

    val reportDF = StateAdminReportJob
      .prepareReport(spark, reporterMock.loadData)
      .cache()

    verify(reporterMock, times(3)).loadData(spark, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace))

    // There are only 2 state information in the test csv
    assert(reportDF.count == 2)
  }
}