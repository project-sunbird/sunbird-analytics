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
  var reporterMock: ReportGenerator = mock[ReportGenerator]
  val sunbirdKeyspace = "sunbird"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    //(jobLoggerMock.isInfoEnabled _).thenReturn(true)
    shadowUserDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/state-admin-report-updater/shadowUserTable.csv")
      .cache()
  }


  "TestUpdateCourseMetrics" should "generate reports" in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace))
      .returning(shadowUserDF).repeat(3)

    val reportDF = StateAdminReportJob
      .prepareReport(spark, reporterMock.loadData)
      .cache()

    // There are only 2 state information in the test csv
    assert(reportDF.count == 2)
  }
}