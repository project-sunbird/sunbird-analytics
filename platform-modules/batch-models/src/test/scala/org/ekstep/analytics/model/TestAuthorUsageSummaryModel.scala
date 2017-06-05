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
        val rdd3 = loadData()
        val events = rdd3.filter { x => (x.uid == "316") }.collect()
        val event3 = events(1).edata.eks.asInstanceOf[Map[String, AnyRef]]
        event3.get("total_session").get.asInstanceOf[Number].longValue() should be(3)
        event3.get("total_ts").get.asInstanceOf[Number].doubleValue() should be(874.8)
        event3.get("ce_total_ts").get.asInstanceOf[Number].doubleValue() should be(0.0)
        event3.get("ce_total_visits").get.asInstanceOf[Number].longValue() should be(7)
        event3.get("ce_visits_occ").get.asInstanceOf[Number].longValue() should be(1)
        event3.get("ce_percent_sessions").get.asInstanceOf[Number].doubleValue() should be(33.33)
        event3.get("avg_session_ts").get.asInstanceOf[Number].doubleValue() should be(291.6)
        event3.get("ce_percent_ts").get.asInstanceOf[Number].doubleValue() should be(0.0)

        //return number of measured events based on time period per author
        rdd3.filter { x => (x.uid == "316") }.count() should be(2)

        //return different time spents for two different period events
        val event5 = events(0).edata.eks.asInstanceOf[Map[String, AnyRef]]
        val event6 = events(1).edata.eks.asInstanceOf[Map[String, AnyRef]]
        event5.get("total_ts").get.asInstanceOf[Number].doubleValue() should be(679.27)
        event6.get("total_ts").get.asInstanceOf[Number].doubleValue() should be(874.8)
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