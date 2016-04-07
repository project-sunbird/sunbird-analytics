package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent


class TestContentSummary extends SparkSpec(null){
  "ContentSummary" should "generate summary and pass all positive test cases" in {

        val rdd = loadFile[MeasuredEvent]("src/test/resources/learner-proficiency/test.log");
        val rdd2 = ContentSummary.execute(rdd, Option(Map("modelVersion" -> "1.4", "modelId" -> "GenericContentSummary")));
        val me = rdd2.collect();
        
        me.length should be(18);

        val event1 = JSONUtils.deserialize[MeasuredEvent](me(1));
        event1.eid should be("ME_CONTENT_SUMMARY");
        //event1.mid should be("06D6C96652BA3F3473661EBC1E2CDCF0");
        event1.context.pdata.model should be("GenericContentSummary");
        event1.context.pdata.ver should be("1.4");
        event1.context.granularity should be("CONTENT");
        event1.context.date_range should not be null;
        
        val eks = event1.edata.eks.asInstanceOf[Map[String,AnyRef]]
        eks.get("averageTsSession").get should be (15.5)
        eks.get("numSessions").get should be (2)
        eks.get("timeSpent").get should be (31.0)
        eks.get("averageInteractionsMin").get should be (8.31)
//        eks.get("numSessionsWeek").get should be ()
//        eks.get("tsWeek").get should be ()
        
  }
}