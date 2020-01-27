package org.ekstep.analytics.model.report

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.{ReportConfig, SparkSpec}
import org.ekstep.analytics.util._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.cloud.storage.BaseStorageService

import scala.collection.mutable.Buffer
import scala.io.Source

class TestCourseEnrollmentModel extends SparkSpec with Matchers with MockFactory {

  implicit val spark: SparkSession = getSparkSession()
  implicit val mockCourseReport: CourseReport = mock[CourseReport]

  var courseBatchDF: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    val cbMapping = sc.textFile("src/test/resources/reports/coursebatch_mapping.json", 1).collect().head
    EmbeddedES.start(Array
    (EsIndex("course-batch",Option("search"), Option(cbMapping),None)))

    EmbeddedES.loadData("course-batch", "search", Buffer(
      """{"courseId":"do_112470675618004992181","participantCount":2,"completedCount":0,"batchId":"0127462617892044804"}""",
      """{"courseId":"0128448115803914244","participantCount":3,"completedCount":3,"batchId":"0127419590263029761308"}""",
      """{"courseId":"05ffe180caa164f56ac193964c5816d4","participantCount":4,"completedCount":3,"batchId":"01273776766975180837"}"""))

    EmbeddedCassandra.loadData("src/test/resources/reports/reports_test_data.cql")
  }

  override def afterAll() {
    super.afterAll()
    EmbeddedES.stop()
  }

  "CourseEnrollmentModel" should "execute Course Enrollment model" in {
    implicit val sqlContext = new SQLContext(sc)
    implicit val mockFc = mock[FrameworkContext]

    val mockStorageService = mock[BaseStorageService]
    (mockFc.getStorageService(_: String)).expects("azure").returns(mockStorageService).anyNumberOfTimes()
    (mockStorageService.upload (_: String, _: String, _: String, _: Option[Boolean], _: Option[Int], _: Option[Int], _: Option[Int])).expects(*, *, *, *, *, *, *).returns("").anyNumberOfTimes();
    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes()

    val config = s"""{
                    |	"reportConfig": {
                    |		"id": "tpd_metrics",
                    |    "metrics" : [],
                    |		"labels": {
                    |			"completionCount": "Completion Count",
                    |			"status": "Status",
                    |			"enrollmentCount": "Enrollment Count",
                    |			"courseName": "Course Name",
                    |			"batchName": "Batch Name",
                    |     "BatchStatus":"Batch Status"
                    |		},
                    |		"output": [{
                    |			"type": "csv",
                    |			"dims": ["identifier", "channel", "name"],
                    |			"fileParameters": ["id", "dims"]
                    |		}]
                    |	},
                    | "esConfig": {
                    | "request": {
                    |        "filters":{
                    |            "objectType": ["Content"],
                    |            "contentType": ["Course"],
                    |            "identifier": [],
                    |            "status": ["Live"]
                    |        },
                    |        "limit": 10000
                    |    }
                    | },
                    |	"key": "druid-reports/",
                    |	"filePath": "src/test/resources/",
                    |	"bucket": "test-container",
                    |	"folderPrefix": ["slug", "reportName"]
                    |}""".stripMargin
    val jobConfig = JSONUtils.deserialize[Map[String, AnyRef]](config)
    //Mock for compositeSearch
    val userdata = JSONUtils.deserialize[CourseDetails](Source.fromInputStream
    (getClass.getResourceAsStream("/tpd-course-report/liveCourse.json")).getLines().mkString).result.content

    import sqlContext.implicits._
    val userDF = userdata.toDF("channel", "identifier", "courseName")
    (mockCourseReport.getLiveCourses(_: Map[String, AnyRef])(_: SparkContext)).expects(jobConfig, *).returns(userDF).anyNumberOfTimes()

    val result = CourseEnrollmentModel.execute(sc.emptyRDD, Option(jobConfig))
    result.count() should be(4)

    result.collect().map(f => {
      f.completionCount should be(0)
    })

    val configMap = jobConfig.get("reportConfig").get.asInstanceOf[Map[String,AnyRef]]
    val reportId = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap)).id

    val slug = result.collect().map(f => f.slug).toList
    val reportName = result.collect().map(_.reportName).toList.head
    slug.head should be ("MPSlug")
    val filePath = jobConfig.get("filePath").get.asInstanceOf[String]
    val key = jobConfig.get("key").get.asInstanceOf[String]
    val outDir = filePath + key + "renamed/" + reportId + "/" + slug.head + "/"
    outDir should be ("src/test/resources/druid-reports/renamed/tpd_metrics/MPSlug/")
  }

  ignore should "fetch course batch details from elastic search" in {

    val df = CourseEnrollmentModel.getCourseBatchCounts("[\"do_112470675618004992181\",\"0128448115803914244\",\"05ffe180caa164f56ac193964c5816d4\"]","[\"0127462617892044804\",\"0127419590263029761308\",\"01273776766975180837\",\"0128448115803914244\",\"f13124c94392dac507bfe36d247e2246\"]")

    assert(df.count() === 3)
    var esData = df.filter(df("courseId") === "do_112470675618004992181").collectAsList().get(0)
    assert(esData.get(0)===2)
    assert(esData.get(1)===0)

    esData = df.filter(df("courseId") === "0128448115803914244").collectAsList().get(0)
    assert(esData.get(0)===3)
    assert(esData.get(1)===3)

  }

}
