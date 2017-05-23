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

    "AuthorUsageSummaryModel" should "generate Total numner of session per author" in {
        val rdd3 = loadData()
        val events = rdd3.collect
        val event3 = events(1).edata.eks.asInstanceOf[Map[String, AnyRef]]
        event3.get("total_session").get.asInstanceOf[Long] should be(1)
    }

    it should " generate total time spent per author" in {
        val rdd3 = loadData()
        val events = rdd3.collect
        val event3 = events(1).edata.eks.asInstanceOf[Map[String, AnyRef]]
        event3.get("total_ts").get.asInstanceOf[Double] should be(17.72)
    }

    it should " generate total time spent on content editor per author" in {
        val rdd3 = loadData()
        val events = rdd3.collect
        val event3 = events(1).edata.eks.asInstanceOf[Map[String, AnyRef]]
        event3.get("ce_total_ts").get.asInstanceOf[Double] should be(0.0)
    }

    it should " generate total number of content editor visit per author" in {
        val rdd3 = loadData()
        val events = rdd3.collect
        val event3 = events(1).edata.eks.asInstanceOf[Map[String, AnyRef]]
        event3.get("ce_total_visits").get.asInstanceOf[Long] should be(2)
    }

    it should " generate how often does a creator log in" in {
        val rdd3 = loadData()
        val events = rdd3.collect
        val event3 = events(1).edata.eks.asInstanceOf[Map[String, AnyRef]]
        event3.get("ce_percent_sessions").get.asInstanceOf[Number].doubleValue() should be(200.0)
    }

    it should " generate what is the average session duration" in {
        val rdd3 = loadData()
        val events = rdd3.collect
        val event3 = events(1).edata.eks.asInstanceOf[Map[String, AnyRef]]
        event3.get("avg_session_ts").get.asInstanceOf[Double] should be(17.72)
    }

    it should " generate percentage of time spent browsing vs. creating" in {
        val rdd3 = loadData()
        val events = rdd3.collect
        val event3 = events(1).edata.eks.asInstanceOf[Map[String, AnyRef]]
        event3.get("ce_percent_ts").get.asInstanceOf[Double] should be(0.0)
    }

    it should "return number of measured events" in {
        val rdd3 = loadData()
        rdd3.count() should be(2)
    }
    it should "return different time spents for two different period events " in {
        val rdd3 = loadData()
        val events = rdd3.collect
        val event3 = events(0).edata.eks.asInstanceOf[Map[String, AnyRef]]
        val event4 = events(1).edata.eks.asInstanceOf[Map[String, AnyRef]]
        event3.get("total_ts").get.asInstanceOf[Double] should be(17.72)
        event4.get("total_ts").get.asInstanceOf[Double] should be(874.8)
    }

    def loadData(): RDD[MeasuredEvent] = {

        val rdd = loadFile[DerivedEvent]("src/test/resources/portal-session-summary/portal-session-summary.log");
        val rdd2 = AuthorUsageSummaryModel.execute(rdd, None);
        rdd2.filter { x => (x.uid == "316") }
    }
}