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

object TextbookSnapshotMetricCreationModel extends MetricsBatchModel[String,String] with Serializable {
  
    implicit val className = "org.ekstep.analytics.model.TextbookSnapshotMetricCreationModel"
    override def name(): String = "TextbookSnapshotMetricCreationModel";
    val event_id = "ME_TEXTBOOK_SNAPSHOT_METRICS"

    def execute(events: RDD[String], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[String] ={
        
        val start_date = jobParams.getOrElse(Map()).getOrElse("start_date", new DateTime().toString(CommonUtil.dateFormat)).asInstanceOf[String];
        val end_date = jobParams.getOrElse(Map()).getOrElse("end_date", start_date).asInstanceOf[String];
        val dispatchParams = JSONUtils.deserialize[Map[String, AnyRef]](AppConf.getConfig("metrics_dispatch_params"));
        
        val groupFn = (x: TextbookSnapshotSummary) => { (x.d_period + "-" + x.d_textbook_id) };
        val details = ConfigDetails(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.TEXTBOOK_SNAPSHOT_METRICS_TABLE, start_date, end_date, AppConf.getConfig("metrics_files_prefix") + event_id.toLowerCase() + "/", ".json", AppConf.getConfig("metrics_dispatch_to"), dispatchParams)
        val res = processQueryAndComputeMetrics(details, groupFn)
        val resRDD = res.mapValues { x =>
            x.map { f =>
                val mid = CommonUtil.getMessageId(event_id, f.d_textbook_id + f.d_period, "DAY", System.currentTimeMillis());
                val event = getMeasuredEvent(event_id, mid, "TextbookSnapshotMetrics", CommonUtil.caseClassToMap(f) - ("d_period", "d_textbook_id"), Dimensions(None, None, None, None, None, None, None, None, None, None, Option(f.d_period), None, None, None, None, None, None, None, None, None, None, None, None, None, None, Option(f.d_textbook_id)))
                JSONUtils.serialize(event)
            }
        }
        val count = saveToS3(resRDD, details)
        JobLogger.log("Textbook snapshot metrics pushed.", Option(Map("count" -> count)), INFO);
        resRDD.map(x => x._2).flatMap { x => x };
    }
}