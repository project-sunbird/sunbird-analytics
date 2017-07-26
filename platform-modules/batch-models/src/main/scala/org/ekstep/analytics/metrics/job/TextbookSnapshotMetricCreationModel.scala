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
import org.ekstep.analytics.updater.TextbookSnapshotSummary
import org.joda.time.DateTime
import org.apache.commons.lang3.StringUtils

object TextbookSnapshotMetricCreationModel extends MetricsBatchModel[String, MeasuredEvent] with Serializable {
  
    implicit val className = "org.ekstep.analytics.model.TextbookSnapshotMetricCreationModel"
    override def name(): String = "TextbookSnapshotMetricCreationModel";
    val event_id = "ME_TEXTBOOK_SNAPSHOT_METRICS"

    def execute(events: RDD[String], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[MeasuredEvent] ={
        
        val start_date = jobParams.getOrElse(Map()).getOrElse("start_date", new DateTime().toString(CommonUtil.dateFormat)).asInstanceOf[String];
        val end_date = jobParams.getOrElse(Map()).getOrElse("end_date", start_date).asInstanceOf[String];
        val dispatchParams = JSONUtils.deserialize[Map[String, AnyRef]](AppConf.getConfig("metrics.dispatch.params"));
        
        val groupFn = (x: TextbookSnapshotSummary) => { (x.d_period + "-" + x.d_textbook_id + "-"  + x.d_app_id + "-" + x.d_channel) };
        val details = ConfigDetails(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.TEXTBOOK_SNAPSHOT_METRICS_TABLE, start_date, end_date, AppConf.getConfig("metrics.creation.dataset.id") + event_id.toLowerCase() + "/", ".json", AppConf.getConfig("metrics.dispatch.to"), dispatchParams)
        val res = processQueryAndComputeMetrics(details, groupFn)
        res.map { x =>
            x._2.map { f =>
                val syncts = CommonUtil.getTimestampOfDayPeriod(f.d_period)
                val mid = CommonUtil.getMessageId(event_id, f.d_textbook_id, "DAY", syncts, Option(f.d_app_id), Option(f.d_channel));
                getMeasuredEvent(event_id, syncts, mid, f.d_channel, "TextbookSnapshotMetrics", CommonUtil.caseClassToMap(f) - ("d_period", "d_textbook_id", "d_app_id", "d_channel", "updated_date"), Dimensions(None, None, None, None, None, None, Option(PData(f.d_app_id, "1.0")), None, None, None, None, Option(f.d_period), None, None, None, None, None, None, None, None, None, None, None, None, None, Option(f.d_textbook_id)))
            }
        }.flatMap { x => x }
    }
}