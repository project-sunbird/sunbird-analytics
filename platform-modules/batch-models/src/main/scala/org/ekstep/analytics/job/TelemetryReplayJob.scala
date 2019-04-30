package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}

import scala.collection.Map

object TelemetryReplayJob extends optional.Application with IJob {

  implicit val className = "org.ekstep.analytics.job.TelemetryReplayJob"

  def name(): String = "TelemetryReplayJob"

  override def main(config: String)(implicit sc: Option[SparkContext]): Unit = {

    JobLogger.start("TelemetryReplayJob Job Started executing", Option(Map("config" -> config, "model" -> name)))
    val totalEvents = process(config)
    JobLogger.end("TelemetryReplayJob Job completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name, "outputEvents" -> totalEvents)))
  }

  def getInputData(config: JobConfig)(implicit mf: Manifest[String], sc: SparkContext): RDD[String] = {
    DataFetcher.fetchBatchData[String](config.search).cache()
  }

  def getSparkContext(jobConfig: JobConfig)(implicit sc: Option[SparkContext]): SparkContext = {

    sc match {
      case Some(value) => {
        value
      }
      case None => {
        CommonUtil.getSparkContext(JobContext.parallelization, jobConfig.appName.getOrElse(jobConfig.model))
      }
    }
  }

  def dispatchData(jobConfig: JobConfig, data: RDD[String])(implicit sc: SparkContext): Long = {
    OutputDispatcher.dispatch(jobConfig.output, data)
  }

  def process(config: String)(implicit sc: Option[SparkContext]): Long = {

    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val sparkContext = getSparkContext(jobConfig)(sc)
    try {
      val data = getInputData(jobConfig)
      dispatchData(jobConfig, data)
    } finally {
      CommonUtil.closeSparkContext()(sparkContext)
    }
  }

}