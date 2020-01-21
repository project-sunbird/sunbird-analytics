package org.ekstep.analytics.model

import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers

class TestCourseEnrollmentModel extends SparkSpec with Matchers with MockFactory {

  implicit val fc = new FrameworkContext()

  ignore should "generate result for course enrollment tpd report" in {
    val query = DruidQueryModel("groupBy","content-model-snapshot","1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00",Option("all"),Option(List(Aggregation(Option("total_live_courses"),"count",""))),Option(List(DruidDimension("identifier",Option("identifier")), DruidDimension("channel",Option("channel")), DruidDimension("name",Option("name")))),Option(List(DruidFilter("equals","status",Option("Live")),DruidFilter("equals","contentType", Option("Course")))))
    val reportConfig2 = ReportConfig("tpd_metrics", "groupBy", QueryDateRange(None, Option("1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00"), Option("all")), List(Metrics("totalliveCourses", "Total Live Courses", query)), Map("total_live_courses" -> "Total Live Courses","courseName" -> "Course Name", "batchName" -> "Batch Name", "status" -> "Status", "enrollmentCount" -> "Enrollment Count", "completionCount" -> "Completion Count"), List(OutputConfig("csv", None, List("total_live_courses"), List("identifier", "channel", "name"))))
    val strConfig2 = JSONUtils.serialize(reportConfig2)
    val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig2), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/", "folderPrefix" -> List("slug","reportName"))
    CourseEnrollmentModel.execute(sc.emptyRDD, Option(modelParams))
  }

}
