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
import org.ekstep.analytics.util.ItemUsageSummaryFact
import org.joda.time.DateTime
import org.apache.commons.lang3.StringUtils

object ItemUsageMetricCreationModel extends MetricsBatchModel[String, MeasuredEvent] with Serializable {
  
    implicit val className = "org.ekstep.analytics.model.ItemUsageMetricCreationModel"
    override def name(): String = "ItemUsageMetricCreationModel";
    val event_id = "ME_ITEM_USAGE_METRICS"

    def execute(events: RDD[String], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[MeasuredEvent] ={
        
        val start_date = jobParams.getOrElse(Map()).getOrElse("start_date", new DateTime().toString(CommonUtil.dateFormat)).asInstanceOf[String];
        val end_date = jobParams.getOrElse(Map()).getOrElse("end_date", start_date).asInstanceOf[String];
        val dispatchParams = JSONUtils.deserialize[Map[String, AnyRef]](AppConf.getConfig("metrics.dispatch.params"));
        
        val groupFn = (x: ItemUsageSummaryFact) => { (x.d_period + "-" + x.d_tag + "-" + x.d_content_id + "-" + x.d_item_id + "-" + x.d_app_id + "-" + x.d_channel) };
        val fetchDetails = ConfigDetails(Constants.CONTENT_KEY_SPACE_NAME, Constants.ITEM_USAGE_SUMMARY_FACT, start_date, end_date, AppConf.getConfig("metrics.consumption.dataset.id") + event_id.toLowerCase() + "/", ".json", AppConf.getConfig("metrics.dispatch.to"), dispatchParams)
        val res = processQueryAndComputeMetrics(fetchDetails, groupFn)
        res.map { x =>
            x._2.map { f =>
                val syncts = CommonUtil.getTimestampOfDayPeriod(f.d_period)
                val mid = CommonUtil.getMessageId(event_id, f.d_content_id + f.d_item_id + f.d_tag, "DAY", syncts, Option(f.d_app_id), Option(f.d_channel));
                getMeasuredEvent(event_id, syncts, mid, f.d_channel, "ItemUsageMetrics", CommonUtil.caseClassToMap(f) - ("d_period", "d_tag", "d_content_id", "d_item_id", "d_app_id", "d_channel", "updated_date"), Dimensions(None, None, None, None, None, None, Option(PData(f.d_app_id, "1.0")), None, None, None, Option(f.d_tag), Option(f.d_period), Option(f.d_content_id), None, Option(f.d_item_id)))
            }
        }.flatMap { x => x }
    }
}