package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.dispatcher.AzureDispatcher
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.util.{Constants, FileUtil}
import org.sunbird.cloud.storage.conf.AppConf

case class CourseInfo(channel: String, courseId: String, courseName: String, total_live_courses: Double)
case class TenantInfo(id: String, slug: String)
case class TenantResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: TenantResult)
case class TenantResult(response: ContentList)
case class ContentList(count: Int, content: List[TenantInfo])
case class ESResponse(took: Double, timed_out: Boolean, _shards: _shards, hits: Hit)
case class _shards(total: Option[Double], successful: Option[Double], skipped: Option[Double], failed: Option[Double])
case class Hit(total: Double, max_score: Double, hits: List[Hits])
case class Hits(_source: _source)
case class _source(batchId: String, courseId: String, status: String, name: String, participantCount: Integer, completedCount: Integer)
case class CourseEnrollmentOutput(date: String, courseName: String, batchName: String, status: String, enrollmentCount: Integer, completionCount: Integer, slug: String, reportName: String) extends AlgoOutput with Output

object CourseEnrollmentModel extends IBatchModelTemplate[Empty, Empty, CourseEnrollmentOutput, CourseEnrollmentOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.model.CourseEnrollmentModel"
  implicit val fc = new FrameworkContext()

  override def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    events
  }

  override def algorithm(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CourseEnrollmentOutput] = {
    val liveCourses = getLiveCoursesFromDruid(config)
    val liveCoursesDruid = liveCourses.map(f => (f.courseId, f))

    val courseBatch = getCourseBatchFromES()
    val courseBatchES= courseBatch.map(f => (f.courseId, f))

    val joinCourses = courseBatchES.leftOuterJoin(liveCoursesDruid)
//    val tenantInfo = getTenantInfo()
//    val tenantInfoAPI = tenantInfo.map(f => (f.id, f))
//
//    val finalRDD = joinCourses.leftOuterJoin(tenantInfoAPI)
//    val date = (new SimpleDateFormat("dd-MM-yyyy")).format(Calendar.getInstance().getTime)
//    finalRDD.map(c=>
//      CourseEnrollmentOutput(date, c._2._1._2.getOrElse(CourseInfo("", "", "", 0.0)).courseName, c._2._1._1.name, c._2._1._1.status,
//        c._2._1._1.participantCount, c._2._1._1.completedCount, c._2._2.getOrElse(TenantInfo("",date)).slug, "course_enrollments"))
    sc.emptyRDD
  }

  override def postProcess(data: RDD[CourseEnrollmentOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CourseEnrollmentOutput] = {

    if (data.count() > 0) {
      val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
      val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))

      reportConfig.metrics.flatMap{c => List()}
      val dimFields = reportConfig.metrics.flatMap { m =>
        if (m.druidQuery.dimensions.nonEmpty) m.druidQuery.dimensions.get.map(f => f.aliasName.getOrElse(f.fieldName))
        else List()
      }

      val labelsLookup = reportConfig.labels ++ Map("date" -> "Date")
      implicit val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      // Using foreach as parallel execution might conflict with local file path
      val key = config.getOrElse("key", null).asInstanceOf[String]
      reportConfig.output.foreach { f =>
        if ("csv".equalsIgnoreCase(f.`type`)) {
          val df = data.toDF().na.fill(0L)
          val metricFields = f.metrics
          val fieldsList = df.columns
          val dimsLabels = labelsLookup.filter(x => f.dims.contains(x._1)).values.toList
          val filteredDf = df.select(fieldsList.head, fieldsList.tail: _*)
          val renamedDf = filteredDf.select(filteredDf.columns.map(c => filteredDf.col(c).as(labelsLookup.getOrElse(c, c))): _*)
          val reportFinalId = if (f.label.nonEmpty && f.label.get.nonEmpty) reportConfig.id + "/" + f.label.get else reportConfig.id
          renamedDf.show()
          val dirPath = writeToCSVAndRename(renamedDf, config ++ Map("dims" -> dimsLabels, "reportId" -> reportFinalId, "fileParameters" -> f.fileParameters))
          AzureDispatcher.dispatchDirectory(config ++ Map("dirPath" -> (dirPath + reportFinalId + "/"), "key" -> (key + reportFinalId + "/")))
        } else {
          val strData = data.map(f => JSONUtils.serialize(f))
          AzureDispatcher.dispatch(strData.collect(), config)
        }
      }
    } else {
      JobLogger.log("No data found from druid", None, Level.INFO)
    }
    data
  }

  def writeToCSVAndRename(data: DataFrame, config: Map[String, AnyRef])(implicit sc: SparkContext): String = {
    val filePath = config.getOrElse("filePath", AppConf.getConfig("spark_output_temp_dir")).asInstanceOf[String]
    val key = config.getOrElse("key", null).asInstanceOf[String]
    val reportId = config.getOrElse("reportId", "").asInstanceOf[String]
    val fileParameters = config.getOrElse("fileParameters", List("")).asInstanceOf[List[String]]
    var dims = config.getOrElse("folderPrefix", List()).asInstanceOf[List[String]]

    dims = if (fileParameters.nonEmpty && fileParameters.contains("date")) dims else dims
    val finalPath = filePath + key.split("/").last

    if(dims.nonEmpty) {
      data.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").partitionBy(dims: _*).mode("overwrite").save(finalPath)
    } else
      data.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(finalPath)

    val renameDir = finalPath + "/renamed/"
    FileUtil.renameHadoopFiles(finalPath, renameDir, reportId, dims)
  }


  def getLiveCoursesFromDruid(config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CourseInfo] = {
    val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))

    val query = reportConfig.metrics.map(f => f.druidQuery)
    val druidResponse = DruidDataFetcher.getDruidData(query(0))
    val response = druidResponse.map{x => JSONUtils.deserialize[Map[String, String]](x)}
    val liveCourses = response.map(c=> CourseInfo(c.getOrElse("channel",""), c.get("identifier").get, c.getOrElse("name",""), c.get("total_live_courses").get.toDouble))
    sc.parallelize(liveCourses)
  }

  def getCourseBatchFromES()(implicit sc: SparkContext) : RDD[_source] = {
    val apiURL = Constants.ELASTIC_SEARCH_SERVICE_ENDPOINT + "/" + Constants.ELASTIC_SEARCH_INDEX_COURSEBATCH_NAME + "/_search"

    val request = s"""{
                     | "query":{
                     |    "match_all":{}
                     |  }
                     |}""".stripMargin
    val ESresponse = RestUtil.post[ESResponse](apiURL, request).hits.hits
    val response = ESresponse.map(f => JSONUtils.deserialize[Hits](JSONUtils.serialize(f))._source)
    sc.parallelize(response)
  }
}
