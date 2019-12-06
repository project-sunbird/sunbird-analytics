package org.ekstep.analytics.job

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.job.report.StateAdminReportJob.className
import org.ekstep.analytics.job.report.{BaseReportSpec, BaseReportsJob, ShadowUserData, StateAdminReportJob}
import org.ekstep.analytics.util.{EmbeddedCassandra, HDFSFileUtils}
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
    assert(reportDF.select("School id").count() == 6)
  }

  it should "Should able to rename the dir" in {
    val fsFileUtils = new HDFSFileUtils(className, JobLogger)
    val files = fsFileUtils.getSubdirectories(s"$AppConf.getConfig('admin.metrics.temp.dir')/renamed")
    files.map { oneChannelDir =>
      val newDirName = oneChannelDir.getParent() + "/" + "Test"
      fsFileUtils.renameDirectory(oneChannelDir.getAbsolutePath(), newDirName)
    }
  }

}