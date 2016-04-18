package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent


class TestContentSummary extends SparkSpec(null){
  "ContentSummary" should "generate contentsummary and pass all positive test cases" in {

        val rdd = loadFile[MeasuredEvent]("src/test/resources/Content-Summary/2016-02-15-20160316.json.gz");
        val rdd2 = ContentSummary.execute(rdd, None);
        val me = rdd2.collect();
        
        me.length should be(23);

        val event1 = JSONUtils.deserialize[MeasuredEvent](me(1));
        println(JSONUtils.serialize(event1))
        event1.eid should be("ME_CONTENT_SUMMARY");
        event1.mid should be("C302E2B4D346A1E0513850753A06B58A");
        event1.context.pdata.model should be("ContentSummary");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        
        val eks = event1.edata.eks.asInstanceOf[Map[String,AnyRef]]
        eks.get("averageTsSession").get should be (3.8)
        eks.get("numSessions").get should be (15)
        eks.get("timeSpent").get should be (57.0)
        eks.get("averageInteractionsMin").get should be (6.706)
        eks.get("numSessionsWeek").get should be (15)
        eks.get("tsWeek").get should be (57.0)
        
        val event2 = JSONUtils.deserialize[MeasuredEvent](me(2));
        println(JSONUtils.serialize(event2))
  }
  
   it should "generate 3 contentsummary" in {

        val rdd = loadFile[MeasuredEvent]("src/test/resources/Content-Summary/dec-2015.log");
        val rdd2 = ContentSummary.execute(rdd, None);
        val me = rdd2.collect();
        
        me.length should be(3);

        val event1 = JSONUtils.deserialize[MeasuredEvent](me(0));
        println(JSONUtils.serialize(event1))
        event1.eid should be("ME_CONTENT_SUMMARY");
        event1.mid should be("4D301C2BBBE16BD2802987A9D72DE18C");
        event1.context.pdata.model should be("ContentSummary");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        
        val eks = event1.edata.eks.asInstanceOf[Map[String,AnyRef]]
        eks.get("averageTsSession").get should be (26.0)
        eks.get("numSessions").get should be (2)
        eks.get("timeSpent").get should be (52.0)
        eks.get("averageInteractionsMin").get should be (4.615)
        eks.get("numSessionsWeek").get should be (2)
        eks.get("tsWeek").get should be (52.0)
        
        val event2 = JSONUtils.deserialize[MeasuredEvent](me(1));
        println(JSONUtils.serialize(event2))
        
        val event3 = JSONUtils.deserialize[MeasuredEvent](me(2));
        println(JSONUtils.serialize(event3))
        
  }
}