package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent


class TestContentSummary extends SparkSpec(null){
  "ContentSummary" should "generate contentsummary and pass all positive test cases" in {

        val rdd = loadFile[MeasuredEvent]("src/test/resources/2016-02-15-20160316.json.gz");
        val rdd2 = ContentSummary.execute(rdd, None);
        val me = rdd2.collect();
        
        me.length should be(3360);

        val event1 = JSONUtils.deserialize[MeasuredEvent](me(1));
        println(JSONUtils.serialize(event1))
        event1.eid should be("ME_CONTENT_SUMMARY");
        event1.mid should be("64377A7220193B41191B8C75D861B7FF");
        event1.context.pdata.model should be("ContentSummary");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        
        val eks = event1.edata.eks.asInstanceOf[Map[String,AnyRef]]
        eks.get("averageTsSession").get should be (52.0)
        eks.get("numSessions").get should be (1)
        eks.get("timeSpent").get should be (52.0)
        eks.get("averageInteractionsMin").get should be (9.23)
        eks.get("numSessionsWeek").get should be (1)
        eks.get("tsWeek").get should be (52.0)
        
        val event2 = JSONUtils.deserialize[MeasuredEvent](me(2));
        println(JSONUtils.serialize(event2))
  }
}