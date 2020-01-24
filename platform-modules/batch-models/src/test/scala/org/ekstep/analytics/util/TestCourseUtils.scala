package org.ekstep.analytics.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.ekstep.analytics.model.SparkSpec
import org.scalamock.scalatest.MockFactory

import scala.io.Source

class TestCourseUtils extends SparkSpec(null) with MockFactory{

  implicit val fc = new FrameworkContext
  val mockCourseReport: CourseReport = mock[CourseReport]
  var spark: SparkSession = _
  var courseBatchDF: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    EmbeddedCassandra.loadData("src/test/resources/reports/reports_test_data.cql") // Load test data in embedded cassandra server
  }

  "CourseUtils" should "execute and give live courses from compositesearch" in {
    implicit val sqlContext = new SQLContext(sc)
    val config = """{"search":{"type":"none"},"model":"org.ekstep.analytics.model.TPDConsumptionMetricsModel","modelParams":{"courseIds":["do_1126981011606323201176", "do_112470675618004992181"],"courseStatus":["Live"],"druidConfig":{"id":"tpd_metrics","labels":{"date":"Date","status":"Batch Status","timespent":"Timespent in mins","courseName":"Course Name","batchName":"Batch Name"},"dateRange":{"staticInterval":"2019-09-08T00:00:00+00:00/2019-09-09T00:00:00+00:00","granularity":"all"},"metrics":[{"metric":"totalCoursePlays","label":"Total Course Plays (in mins)","druidQuery":{"queryType":"groupBy","dataSource":"summary-events","intervals":"2019-09-08/2019-09-09","aggregations":[{"name":"sum__edata_time_spent","type":"doubleSum","fieldName":"edata_time_spent"}],"dimensions":[{"fieldName":"object_rollup_l1","aliasName":"courseId"},{"fieldName":"uid","aliasName":"userId"},{"fieldName":"context_cdata_id","aliasName":"batchId"}],"filters":[{"type":"equals","dimension":"eid","value":"ME_WORKFLOW_SUMMARY"},{"type":"in","dimension":"dimensions_pdata_id","values":["dev.sunbird.app","dev.sunbird.portal"]},{"type":"equals","dimension":"dimensions_type","value":"content"},{"type":"equals","dimension":"dimensions_mode","value":"play"},{"type":"equals","dimension":"context_cdata_type","value":"batch"}],"postAggregation":[{"type":"arithmetic","name":"timespent","fields":{"leftField":"sum__edata_time_spent","rightField":60,"rightFieldType":"constant"},"fn":"/"}],"descending":"false"}}],"output":[{"type":"csv","metrics":["timespent"],"dims":["identifier","channel","name"],"fileParameters":["id","dims"]}],"queryType":"groupBy"},"key":"druid-reports/","filePath":"src/test/resources/","bucket":"test-container","folderPrefix":["slug","reportName"]},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"TPD Course Consumption Metrics Model","deviceMapping":false}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](config).modelParams

    val userdata = JSONUtils.deserialize[CourseDetails](Source.fromInputStream
    (getClass.getResourceAsStream("/tpd-course-report/liveCourse.json")).getLines().mkString).result.content

    import sqlContext.implicits._
    val userDF = userdata.toDF("channel", "identifier", "courseName")
    (mockCourseReport.getLiveCourses(_: Map[String, AnyRef])(_: SparkContext)).expects(jobConfig.get, *).returns(userDF).anyNumberOfTimes()
  }
}
