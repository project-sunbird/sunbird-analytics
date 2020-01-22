package org.ekstep.analytics.model.report

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.ekstep.analytics.model.ReportConfig
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.dispatcher.AzureDispatcher
import org.ekstep.analytics.job.report.{BaseCourseMetrics, BaseCourseMetricsOutput}
import org.ekstep.analytics.util.{Constants, CourseUtils, FileUtil, TenantInfo}
import org.sunbird.cloud.storage.conf.AppConf


case class CourseEnrollmentOutput(date: String, courseName: String, batchName: String, status: String, enrollmentCount: Integer, completionCount: Integer, slug: String, reportName: String) extends AlgoOutput with Output

object CourseEnrollmentModel extends BaseCourseMetrics[Empty, BaseCourseMetricsOutput, CourseEnrollmentOutput, CourseEnrollmentOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.model.CourseEnrollmentModel"
  implicit val fc = new FrameworkContext()

  override def algorithm(events: RDD[BaseCourseMetricsOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CourseEnrollmentOutput] = {
    events
    println("testing flow")
    sc.parallelize(List(CourseEnrollmentOutput("","","","",2,2,"","")))
  }

  override def postProcess(data: RDD[CourseEnrollmentOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CourseEnrollmentOutput] = {

    if (data.count() > 0) {
      val configMap = config("druidConfig").asInstanceOf[Map[String, AnyRef]]
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
          val dirPath = CourseUtils.writeToCSVAndRename(renamedDf, config ++ Map("dims" -> dimsLabels, "reportId" -> reportFinalId, "fileParameters" -> f.fileParameters))
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
}
