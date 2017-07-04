package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.util.SessionBatchModel

class TestMonitorSummaryModel extends SparkSpec(null) {

    "Monitor Summary Model" should "monitor the data products logs" in {

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/monitor-summary/2017-06-28.log");
        val rdd2 = MonitorSummaryModel.execute(rdd1, None);
        val eks_map = rdd2.first().edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks_map.get("jobs_completed_count").get.asInstanceOf[Number].longValue() should be(58)
        eks_map.get("total_events_generated").get.asInstanceOf[Number].longValue() should be(16128)
        eks_map.get("jobs_failed_count").get.asInstanceOf[Number].longValue() should be(1)
        eks_map.get("total_ts").get.asInstanceOf[Number].doubleValue() should be(2422755.0)
        eks_map.get("jobs_start_count").get.asInstanceOf[Number].longValue() should be(59)
    }
}