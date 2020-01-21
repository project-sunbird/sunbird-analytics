package org.ekstep.analytics.model

import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.{EmbeddedES, EsIndex}
import org.scalamock.scalatest.MockFactory

import scala.collection.mutable.Buffer


class TestTPDConsumptionMetricsModel extends SparkSpec(null) with MockFactory{


  implicit var spark: SparkSession = _

  override def beforeAll() = {
    super.beforeAll()
    spark = getSparkSession()

    import org.joda.time.DateTimeUtils
    DateTimeUtils.setCurrentMillisFixed(1531047600000L)
    val courseBatchMapping = spark.sparkContext.textFile("src/test/resources/reports/courseBatch.json", 1).collect().head
    println("*****Start elasticsearch************")
    EmbeddedES.start(Array(
              EsIndex("course-batch", Option("/_search"), None, Option(courseBatchMapping))
    ))

    EmbeddedES.loadData("course-batch", "/_search", Buffer(
      """{"batchId": "0127462617892044804","courseId": "do_112470675618004992181","status": 2,"name": "C Programming Tutorial For Beginners -4",}""",
      """{"batchId": "0127419590263029761308","courseId": "do_112470675618004992181","status": 1,"name": "s",}"""
    ))
  }

  override def afterAll() {
    //super.afterAll();
    EmbeddedES.stop()
  }

  implicit val fc = new FrameworkContext()

   ignore should "execute the druid data fetcher for this config" in {
     val config = """{"search":{"type":"none"},"model":"org.ekstep.analytics.model.TPDConsumptionMetricsModel","modelParams":{"reportConfig":{"id":"tpd_metrics","labels":{"date":"Date","status":"Batch Status","timespent":"Timespent in mins","courseName":"Course Name","batchName":"Batch Name"},"dateRange":{"staticInterval":"2019-09-08T00:00:00+00:00/2019-09-09T00:00:00+00:00","granularity":"all"},"metrics":[{"metric":"totalCoursePlays","label":"Total Course Plays (in mins)","druidQuery":{"queryType":"groupBy","dataSource":"summary-events","intervals":"2019-09-08/2019-09-09","aggregations":[{"name":"sum__edata_time_spent","type":"doubleSum","fieldName":"edata_time_spent"}],"dimensions":[{"fieldName":"object_rollup_l1","aliasName":"courseId"},{"fieldName":"uid","aliasName":"userId"},{"fieldName":"context_cdata_id","aliasName":"batchId"}],"filters":[{"type":"equals","dimension":"eid","value":"ME_WORKFLOW_SUMMARY"},{"type":"in","dimension":"dimensions_pdata_id","values":["dev.sunbird.app","dev.sunbird.portal"]},{"type":"equals","dimension":"dimensions_type","value":"content"},{"type":"equals","dimension":"dimensions_mode","value":"play"},{"type":"equals","dimension":"context_cdata_type","value":"batch"}],"postAggregation":[{"type":"arithmetic","name":"timespent","fields":{"leftField":"sum__edata_time_spent","rightField":60,"rightFieldType":"constant"},"fn":"/"}],"descending":"false"}}],"output":[{"type":"csv","metrics":["timespent"],"dims":["identifier","channel","name"],"fileParameters":["id","dims"]}],"queryType":"groupBy"},"key":"druid-reports/","filePath":"src/test/resources/","bucket":"test-container","folderPrefix":["slug","reportName"]},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"TPD Course Consumption Metrics Model","deviceMapping":false}"""
//     val jobConfig =  JSONUtils.deserialize[JobConfig](config).modelParams
     //     TPDConsumptionMetricsModel.execute(sc.emptyRDD, jobConfig)
     val query = DruidQueryModel("groupBy","summary-events","2019-09-08/2019-09-09",Option("all"),Option(List(Aggregation(Option("sum__edata_time_spent"),"doubleSum","edata_time_spent"))),Option(List(DruidDimension("object_rollup_l1",Option("courseId")), DruidDimension("uid",Option("userId")), DruidDimension("context_cdata_id",Option("batchId")))),Option(List(DruidFilter("equals","eid",Option("ME_WORKFLOW_SUMMARY")),DruidFilter("equals","dimensions_type", Option("content")), DruidFilter("equals","context_cdata_type", Option("batch")), DruidFilter("equals","dimensions_mode", Option("play")), DruidFilter("in","dimensions_pdata_id", None,Option(List("dev.sunbird.app", "dev.sunbird.portal"))))), None, Option(List(PostAggregation("arithmetic", "timespent", PostAggregationFields("sum__edata_time_spent", 60.asInstanceOf[AnyRef], "constant"), "/" ))))
     val reportConfig2 = ReportConfig("tpd_consumption_metrics", "groupBy", QueryDateRange(None, Option("2019-09-08T00:00:00+00:00/2019-09-09T00:00:00+00:00"), Option("all")), List(Metrics("totalCoursePlays", "Total Course Plays (in mins)", query)), Map("timespent" -> "Timespent in mins","courseName" -> "Course Name", "name" -> "Batch Name", "status" -> "Batch Status", "date" -> "Date"), List(OutputConfig("csv", None, List("timespent"), List("courseId", "userId", "batchId"))))
     val strConfig2 = JSONUtils.serialize(reportConfig2)
     val modelParams = Map("reportConfig" -> JSONUtils.deserialize[Map[String, AnyRef]](strConfig2), "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/", "folderPrefix" -> List("slug","reportName"))
     TPDConsumptionMetricsModel.execute(sc.emptyRDD, Option(modelParams))
   }

   it should "fetch the course-batch details from the elasticsearch" in {
//     implicit val spark = SparkSession.builder().getOrCreate()
     val courseIds = List("do_112470675618004992181","f13124c94392dac507bfe36d247e2246")
     val batchIds = List("0127419590263029761308","0127462617892044804")
     val courseBatch = TPDConsumptionMetricsModel.getCourseBatchFromES(JSONUtils.serialize(courseIds), JSONUtils.serialize(batchIds))
     courseBatch.count() should be(2)
   }
}
