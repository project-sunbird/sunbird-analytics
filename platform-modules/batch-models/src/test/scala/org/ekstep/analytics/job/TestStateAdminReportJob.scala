package org.ekstep.analytics.job

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.model.SparkSpec
import org.mockito.Mockito
import org.mockito.Mockito._
import org.neo4j.kernel.impl.store.UnderlyingStorageException
import org.scalamock.scalatest.MockFactory
import org.scalatest.mockito.MockitoSugar

import scala.collection.Map


class TestStateAdminReportJob extends SparkSpec(null) with MockitoSugar {

  var spark: SparkSession = _
  var shadowUserDF: DataFrame = _
  var locationDF: DataFrame = _
  var orgDF: DataFrame = _
  var reporterMock: AdminReportGenerator = mock[AdminReportGenerator](Mockito.withSettings().serializable())
  val sunbirdKeyspace = "sunbird"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    val shadowUserEncoder = Encoders.product[ShadowUserData].schema
    shadowUserDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .schema(shadowUserEncoder)
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

    when(reporterMock.loadDataWithSchema(spark, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace), Some(shadowUserEncoder)))
      .thenReturn(shadowUserDF, shadowUserDF, shadowUserDF)
      .thenThrow(new RuntimeException("Called more than 3 timess"))
    when(reporterMock.loadDataWithSchema(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace), None)).thenReturn(locationDF)
    when(reporterMock.loadDataWithSchema(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace), None))
      .thenReturn(orgDF, orgDF, orgDF, orgDF, orgDF, orgDF, orgDF)
      .thenThrow(new RuntimeException("Called more than 7 timess"))
  }


  "TestUpdateStateAdminReport" should "generate reports" in {
    val reportDF = StateAdminReportJob
      .generateReport(spark, reporterMock.loadDataWithSchema)
      .cache()

    verify(reporterMock, times(3)).loadData(spark, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace))

    // There are only 2 state information in the test csv
    assert(reportDF.count == 2)
  }
}