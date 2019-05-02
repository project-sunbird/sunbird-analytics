package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}

import scala.collection.Map

object EventsReplayJob extends optional.Application with IJob {

  implicit val className = "org.ekstep.analytics.job.EventsReplayJob"

  def name(): String = "EventsReplayJob"

  override def main(config: String)(implicit sc: Option[SparkContext]): Unit = {

    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    val jobName = jobConfig.appName.getOrElse(name)
    JobLogger.init(jobName)
    JobLogger.start(jobName + " Started executing", Option(Map("config" -> config, "model" -> name)))
    val totalEvents = process(jobConfig)
    JobLogger.end(jobName + " Completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name, "outputEvents" -> totalEvents)))
  }

  def getInputData(config: JobConfig)(implicit mf: Manifest[String], sc: SparkContext): RDD[String] = {
    DataFetcher.fetchBatchData[String](config.search).cache()
  }

  def getSparkContext(jobConfig: JobConfig)(implicit sc: Option[SparkContext]): SparkContext = {
    sc.getOrElse(CommonUtil.getSparkContext(JobContext.parallelization, jobConfig.appName.getOrElse(jobConfig.model)))
  }

  def dispatchData(jobConfig: JobConfig, data: RDD[String])(implicit sc: SparkContext): Long = {
    OutputDispatcher.dispatch(jobConfig.output, data)
  }

  def process(jobConfig: JobConfig)(implicit sc: Option[SparkContext]): Long = {

    implicit val sparkContext = getSparkContext(jobConfig)(sc)
    try {
      val data = getInputData(jobConfig)
      dispatchData(jobConfig, data)
    } finally {
      CommonUtil.closeSparkContext()(sparkContext)
    }
  }

}