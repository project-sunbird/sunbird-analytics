package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent


class TestContentSummary extends SparkSpec(null){
  "ContentSummary" should "generate summary and pass all positive test cases" in {

        val rdd = loadFile[MeasuredEvent]("src/test/resources/2016-02-15-20160316.json.gz");
        val rdd2 = ContentSummary.execute(rdd, Option(Map("modelVersion" -> "1.4", "modelId" -> "GenericContentSummary")));
        val me = rdd2.collect();
        
        me.length should be(23);

        val event1 = JSONUtils.deserialize[MeasuredEvent](me(1));
        println(JSONUtils.serialize(event1))
        event1.eid should be("ME_CONTENT_SUMMARY");
        //event1.mid should be("06D6C96652BA3F3473661EBC1E2CDCF0");
        event1.context.pdata.model should be("GenericContentSummary");
        event1.context.pdata.ver should be("1.4");
        event1.context.granularity should be("CONTENT");
        event1.context.date_range should not be null;
        
        val eks = event1.edata.eks.asInstanceOf[Map[String,AnyRef]]
        eks.get("averageTsSession").get should be (127.66666666666667)
        eks.get("numSessions").get should be (3)
        eks.get("timeSpent").get should be (383.0)
        eks.get("averageInteractionsMin").get should be (32.123333333333335)
        eks.get("numSessionsWeek").get should be (3)
        eks.get("tsWeek").get should be (383.0)
        
  }
}