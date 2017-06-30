package org.ekstep.analytics.metrics.job

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.util.ConfigDetails
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.updater.TextbookSessionMetricsFact
import org.joda.time.DateTime

object TextbookUsageMetricCreationModel extends MetricsBatchModel[String,String] with Serializable {
  
    implicit val className = "org.ekstep.analytics.model.TextbookUsageMetricCreationModel"
    override def name(): String = "TextbookUsageMetricCreationModel";
    val event_id = "ME_TEXTBOOK_CREATION_METRICS"

    def execute(events: RDD[String], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[String] ={
        
        val start_date = jobParams.getOrElse(Map()).getOrElse("start_date", new DateTime().toString(CommonUtil.dateFormat)).asInstanceOf[String];
        val end_date = jobParams.getOrElse(Map()).getOrElse("end_date", start_date).asInstanceOf[String];
        val dispatchParams = JSONUtils.deserialize[Map[String, AnyRef]](AppConf.getConfig("metrics.dispatch.params"));
        
        val groupFn = (x: TextbookSessionMetricsFact) => { (x.d_period + "-" + x.d_app_id + "-" + x.d_channel_id) };
        val details = ConfigDetails(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.TEXTBOOK_SESSION_METRICS_FACT, start_date, end_date, AppConf.getConfig("metrics.creation.dataset.id") + event_id.toLowerCase() + "/", ".json", AppConf.getConfig("metrics.dispatch.to"), dispatchParams)
        val res = processQueryAndComputeMetrics(details, groupFn)
        val resRDD = res.mapValues { x =>
            x.map { f =>
                val mid = CommonUtil.getMessageId(event_id, f.d_period.toString(), "DAY", System.currentTimeMillis(), Option(f.d_app_id), Option(f.d_channel_id));
                val event = getMeasuredEvent(event_id, mid, "TextbookCreationMetrics", CommonUtil.caseClassToMap(f) - ("d_period", "d_app_id", "d_channel_id", "updated_date"), Dimensions(None, None, None, None, None, None, None, None, None, None, Option(f.d_period), None, None, None, None, None, None, None, None, None, None, None, None, Option(f.d_app_id), None, None, Option(f.d_channel_id)))
                JSONUtils.serialize(event)
            }
        }
        val count = saveToS3(resRDD, details)
        JobLogger.log("Textbook creation metrics pushed.", Option(Map("count" -> count)), INFO);
        resRDD.map(x => x._2).flatMap { x => x };
    }
}