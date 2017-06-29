package org.ekstep.analytics.model
/**
 * @author Yuva
 */
import org.ekstep.analytics.creation.model.CreationEvent
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.SessionBatchModel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.DerivedEvent

class TestMonitorSummaryModel extends SparkSpec(null) {

    "Monitor Summary Model" should "monitor the data products logs" in {

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/monitor-summary/joblog-05.log");
        val rdd2 = MonitorSummaryModel.execute(rdd1, None);
        val eks_map = rdd2.first().edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks_map.get("jobs_completed_count").get.asInstanceOf[Number].longValue() should be(2)
        eks_map.get("total_events_generated").get.asInstanceOf[Number].longValue() should be(7)
        eks_map.get("jobs_failed_count").get.asInstanceOf[Number].longValue() should be(0)
        eks_map.get("total_ts").get.asInstanceOf[Number].doubleValue() should be(0.0)
        eks_map.get("jobs_start_count").get.asInstanceOf[Number].longValue() should be(2)
        val job_summary = eks_map.get("jobs_summary").get.asInstanceOf[Array[Map[String, AnyRef]]](0)
        job_summary.get("model").get should be("TextbookSessionSummaryModel")
        job_summary.get("time_taken").get.asInstanceOf[Number].doubleValue() should be(0.0)
        job_summary.get("output_count").get.asInstanceOf[Number].longValue() should be(3)
        job_summary.get("status").get should be("SUCCESS")
        job_summary.get("input_count").get.asInstanceOf[Number].longValue() should be(74)
    }
}