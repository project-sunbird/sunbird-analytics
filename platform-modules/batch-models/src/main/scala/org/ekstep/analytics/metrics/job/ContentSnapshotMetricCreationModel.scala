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
import org.ekstep.analytics.updater.ContentSnapshotSummary
import org.joda.time.DateTime

object ContentSnapshotMetricCreationModel extends MetricsBatchModel[String,String] with Serializable {
  
    implicit val className = "org.ekstep.analytics.model.ContentSnapshotMetricCreationModel"
    override def name(): String = "ContentSnapshotMetricCreationModel";
    val event_id = "ME_CONTENT_SNAPSHOT_METRICS"

    def execute(events: RDD[String], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[String] ={
        
        val start_date = jobParams.getOrElse(Map()).getOrElse("start_date", new DateTime().toString(CommonUtil.dateFormat)).asInstanceOf[String];
        val end_date = jobParams.getOrElse(Map()).getOrElse("end_date", start_date).asInstanceOf[String];
        val dispatchParams = JSONUtils.deserialize[Map[String, AnyRef]](AppConf.getConfig("metrics.dispatch.params"));
        
        val groupFn = (x: ContentSnapshotSummary) => { (x.d_period + "-" + x.d_author_id + "-" + x.d_partner_id) };
        val fetchDetails = ConfigDetails(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SNAPSHOT_SUMMARY, start_date, end_date, AppConf.getConfig("metrics.consumption.dataset.id") + event_id.toLowerCase() + "/", ".json", AppConf.getConfig("metrics.dispatch.to"), dispatchParams)
        val res = processQueryAndComputeMetrics(fetchDetails, groupFn)
        val resRDD = res.mapValues { x =>
            x.map { f =>
                val mid = CommonUtil.getMessageId(event_id, f.d_author_id + f.d_partner_id + f.d_period, "DAY", System.currentTimeMillis(), None, None);
                val event = getMeasuredEvent(event_id, mid, "", "ContentSnapshotMetrics", CommonUtil.caseClassToMap(f) - ("d_period", "d_author_id", "d_partner_id", "updated_date"), Dimensions(None, None, None, None, None, None, None, None, None, None, None, Option(f.d_period), None, None, None, None, None, None, None, None, None, Option(f.d_author_id), Option(f.d_partner_id), None, None))
                JSONUtils.serialize(event)
            }
        }
        val count = saveToS3(resRDD, fetchDetails)
        JobLogger.log("Content Snapshot metrics pushed.", Option(Map("count" -> count)), INFO);
        resRDD.map(x => x._2).flatMap { x => x };
    }
}