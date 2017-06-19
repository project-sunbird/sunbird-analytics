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
import org.ekstep.analytics.updater.ConceptSnapshotSummary

object ConceptSnapshotMetricCreationModel extends MetricsBatchModel[String,String] with Serializable {
  
    implicit val className = "org.ekstep.analytics.model.ContentSnapshotMetricCreationModel"
    override def name(): String = "ContentSnapshotMetricCreationModel";
    val event_id = "ME_CONCEPT_SNAPSHOT_METRICS"

    def execute(events: RDD[String], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[String] ={
        
        val start_date = jobParams.getOrElse(Map()).getOrElse("start_date", 0L).asInstanceOf[Long];
        val end_date = jobParams.getOrElse(Map()).getOrElse("end_date", 0L).asInstanceOf[Long];
        val dispatchParams = JSONUtils.deserialize[Map[String, AnyRef]](AppConf.getConfig("metrics_dispatch_params"));
        
        val groupFn = (x: ConceptSnapshotSummary) => { (x.d_period + "-" + x.d_concept_id) };
        val fetchDetails = ConfigDetails(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONCEPT_SNAPSHOT_SUMMARY, start_date, end_date, AppConf.getConfig("metrics_files_prefix") + event_id.toLowerCase() + "/", ".json", AppConf.getConfig("metrics_dispatch_to"), dispatchParams)
        val res = processQueryAndComputeMetrics(fetchDetails, groupFn)
        val resRDD = res.mapValues { x =>
            x.map { f =>
                val event = getMeasuredEvent(event_id, "ConceptSnapshotMetrics", CommonUtil.caseClassToMap(f) - ("d_period", "d_concept_id"), Dimensions(None, None, None, None, None, None, None, None, None, None, Option(f.d_period), None, None, None, None, None, None, None, None, None, None, None, Option(f.d_concept_id), None))
                JSONUtils.serialize(event)
            }
        }
        val count = saveToS3(resRDD, fetchDetails)
        JobLogger.log("Concept Snapshot metrics pushed.", Option(Map("count" -> count)), INFO);
        events
    }
}