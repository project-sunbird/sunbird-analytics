package org.ekstep.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.{Empty, FrameworkContext, IBatchModelTemplate}
import org.ekstep.analytics.util.CourseUtils

trait BaseCourseMetricsJob extends IBatchModelTemplate[Empty,Empty,Empty,Empty] {

  def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    val liveCourses = CourseUtils.getLiveCourses(config)
    events
  }

  def algorithm(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty]

  def postProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty]

}
