package org.ekstep.analytics.model

import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher

class TestContentUsageSummary extends SparkSpec(null) {

    it should "generate content summary events for (all, per content, per tag, per tag & per content) diemnsions" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/content-usage-summary/test.log");
        val tagList = Array("becb887fe82f24c644482eb30041da6d88bd8150", "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb")
        val rdd2 = ContentUsageSummary.execute(rdd, Option(Map("tagList" -> tagList)));
        val events = rdd2.collect

        // all summary testing
        val allSum = events.filter { x => (x.dimensions.gdata == None) && (x.dimensions.tag == None) }
        allSum.length should be(36)
        val allSumEvent = allSum.last
        allSumEvent.eid should be("ME_CONTENT_USAGE_SUMMARY")
        allSumEvent.mid should be("A1811DAD116035D4900FA7F1924F5B33")
        val eksMap = allSumEvent.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap.get("avg_ts").get.asInstanceOf[Double] should be(120.63)
        eksMap.get("total_sessions").get.asInstanceOf[Long] should be(8L)
        eksMap.get("avg_interactions_min").get.asInstanceOf[Double] should be(12.25)
        eksMap.get("total_interactions").get.asInstanceOf[Long] should be(197L)
        eksMap.get("total_ts").get.asInstanceOf[Double] should be(965)

        // per tag Summary
        val perTagSum = events.filter { x => (x.dimensions.gdata == None) && (x.dimensions.tag != None) }
        perTagSum.length should be(48)

        // per content summary
        val perContentSum = events.filter { x => (x.dimensions.gdata != None) && (x.dimensions.tag == None) }
        perContentSum.length should be(190)
        val do_30022619Sum = perContentSum.filter { x => "do_30022619".equals(x.dimensions.gdata.get.id) }
        do_30022619Sum.length should be(1)

        val perContentEksMap = do_30022619Sum.last.edata.eks.asInstanceOf[Map[String, AnyRef]]
        perContentEksMap.get("avg_ts").get.asInstanceOf[Double] should be(261.29)
        perContentEksMap.get("total_sessions").get.asInstanceOf[Long] should be(18L)
        perContentEksMap.get("avg_interactions_min").get.asInstanceOf[Double] should be(21.97)
        perContentEksMap.get("total_interactions").get.asInstanceOf[Long] should be(1722L)
        perContentEksMap.get("total_ts").get.asInstanceOf[Double] should be(4703.13)

    }

    //    it should "generate content summary from input events with zero timeSpent and non-zero noOfInteractEvents for multiple content_id" in {
    //        val rdd = loadFile[DerivedEvent]("src/test/resources/content-usage-summary/test_data2.log");
    //        val rdd2 = ContentUsageSummary.execute(rdd, None);
    //        val events = rdd2.collect
    //        events.length should be(4)
    //
    //        for (summ <- events) {
    //            val eksMap = summ.edata.eks.asInstanceOf[Map[String, AnyRef]]
    //            eksMap.get("total_ts").get should not be (0)
    //            eksMap.get("avg_ts_session").get should not be (0)
    //            eksMap.get("total_interactions").get should be(0)
    //            eksMap.get("avg_interactions_min").get should be(0)
    //        }
    //    }
    //
    //    it should "generate content summary from input events with non-zero timeSpent and non-zero noOfInteractEvents" in {
    //        val rdd = loadFile[DerivedEvent]("src/test/resources/content-usage-summary/test_data3.log");
    //        val rdd2 = ContentUsageSummary.execute(rdd, None);
    //
    //        val events = rdd2.collect
    //        events.length should be(3)
    //
    //        for (summ <- events) {
    //            val eksMap = summ.edata.eks.asInstanceOf[Map[String, AnyRef]]
    //            eksMap.get("total_ts").get should not be (0)
    //            eksMap.get("avg_ts_session").get should not be (0)
    //            eksMap.get("total_interactions").get should not be (0)
    //            eksMap.get("avg_interactions_min").get should not be (0)
    //        }
    //    }
    //
    //    it should "generate content summary from input events with non-zero timeSpent and non-zero noOfInteractEvents and groupUser = true" in {
    //        val rdd = loadFile[DerivedEvent]("src/test/resources/content-usage-summary/test_data4.log");
    //        val rdd2 = ContentUsageSummary.execute(rdd, None);
    //
    //        val events = rdd2.collect
    //        events.length should be(5)
    //
    //        for (summ <- events) {
    //            val eksMap = summ.edata.eks.asInstanceOf[Map[String, AnyRef]]
    //            eksMap.get("total_ts").get should not be (0)
    //            eksMap.get("avg_ts_session").get should not be (0)
    //            eksMap.get("total_interactions").get should not be (0)
    //            eksMap.get("avg_interactions_min").get should not be (0)
    //        }
    //    }
}