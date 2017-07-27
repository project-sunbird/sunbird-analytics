package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.DerivedEvent
import org.apache.spark.rdd.RDD
/**
 * @author yuva
 */
class TestAuthorUsageSummaryModel extends SparkSpec(null) {

    "AuthorUsageSummaryModel" should "generate metrics per author" in {
        val rdd = loadData()
        val events = rdd.filter { x => (x.uid == "316") }.collect()
        
        events.size should be(2)
        
        val event1 = events(0).edata.eks.asInstanceOf[Map[String, AnyRef]]
        event1.get("total_sessions").get.asInstanceOf[Number].longValue() should be(3)
        event1.get("total_ts").get.asInstanceOf[Number].doubleValue() should be(874.8)
        event1.get("ce_total_ts").get.asInstanceOf[Number].doubleValue() should be(0.0)
        event1.get("ce_total_visits").get.asInstanceOf[Number].longValue() should be(7)
        event1.get("ce_visits_count").get.asInstanceOf[Number].longValue() should be(1)
        event1.get("ce_percent_sessions").get.asInstanceOf[Number].doubleValue() should be(33.33)
        event1.get("avg_ts_session").get.asInstanceOf[Number].doubleValue() should be(291.6)
        event1.get("ce_percent_ts").get.asInstanceOf[Number].doubleValue() should be(0.0)

        val event2 = events(1).edata.eks.asInstanceOf[Map[String, AnyRef]]
        event2.get("total_sessions").get.asInstanceOf[Number].longValue() should be(2)
        event2.get("total_ts").get.asInstanceOf[Number].doubleValue() should be(679.27)
        event2.get("ce_total_ts").get.asInstanceOf[Number].doubleValue() should be(0.0)
        event2.get("ce_total_visits").get.asInstanceOf[Number].longValue() should be(2)
        event2.get("ce_visits_count").get.asInstanceOf[Number].longValue() should be(1)
        event2.get("ce_percent_sessions").get.asInstanceOf[Number].doubleValue() should be(50.0)
        event2.get("avg_ts_session").get.asInstanceOf[Number].doubleValue() should be(339.64)
        event2.get("ce_percent_ts").get.asInstanceOf[Number].doubleValue() should be(0.0)
    }

    it should "not generate events if anonymous_user is true " in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/author-usage-summary/portal-session-summary1.log");
        val rdd1 = AuthorUsageSummaryModel.execute(rdd, None);
        rdd1.isEmpty() should be(true)
    }

    def loadData(): RDD[MeasuredEvent] = {
        val rdd = loadFile[DerivedEvent]("src/test/resources/author-usage-summary/portal-session-summary.log");
        AuthorUsageSummaryModel.execute(rdd, None);
    }
}