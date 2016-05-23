package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent

class TestGenieUsageSummary extends SparkSpec(null) {

    it should "generate content summary events" in {
        val rdd = loadFile[Event]("src/test/resources/genie_usage_summary/2016-04-25-1461584566696.json");
        val rdd2 = GenieUsageSummary.execute(rdd, None);
        val events = rdd2.collect
        events.size should be(82)
        val gse = events.filter { x => x.contains("ME_GENIE_SUMMARY") }
        val gsse = events.filter { x => x.contains("ME_GENIE_SESSION_SUMMARY") }
        gse.size should be(36)
        gsse.size should be(46)

        // check the number of events where timeSpent==0 
        val zeroTimeSpentGE = gse.map { x => JSONUtils.deserialize[MeasuredEvent](x).edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double] }.filter { x => 0 == x }
        zeroTimeSpentGE.size should be(5)
        
        val zeroTimeSpentGS = gsse.map { x => JSONUtils.deserialize[MeasuredEvent](x).edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double] }.filter { x => 0 == x }
        zeroTimeSpentGS.size should be(6)

        val event1 = JSONUtils.deserialize[MeasuredEvent](gse.last)
        val eksMap1 = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]

        eksMap1.get("timeSpent").get.asInstanceOf[Double] should be(9d)
        eksMap1.get("time_stamp").get.asInstanceOf[Long] should be(1460526898000l)
        eksMap1.get("contentCount").get.asInstanceOf[Int] should be(0)

        val event2 = JSONUtils.deserialize[MeasuredEvent](gsse(2))
        val eksMap2 = event2.edata.eks.asInstanceOf[Map[String, AnyRef]]

        eksMap2.get("timeSpent").get.asInstanceOf[Double] should be(848.0d)
        eksMap2.get("time_stamp").get.asInstanceOf[Long] should be(1461478842000l)
        eksMap2.get("contentCount").get.asInstanceOf[Int] should be(7)
    }
}