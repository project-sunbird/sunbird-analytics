package org.ekstep.analytics.model

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.util.JSONUtils

class TestDeviceUsageSummary extends SparkSpec(null) {
  
  "DeviceUsageSummary" should "generate DeviceUsageSummary events from a sample file and pass all positive test cases " in {
    
        val rdd1 = loadFile[MeasuredEvent]("src/test/resources/device-usage-summary/test_data_1.log");
        val rdd2 = DeviceUsageSummary.execute(rdd1, Option(Map("modelVersion" -> "1.0", "modelId" -> "DeviceUsageSummarizer","granularity" -> "DAY")));
        val me = rdd2.collect()
        me.length should be(1)
        val event1 = JSONUtils.deserialize[MeasuredEvent](me(0));
        
        event1.eid should be("ME_DEVICE_USAGE_SUMMARY");
        event1.mid should be("0b303d4d66d13ad0944416780e52cc3db1feba87");
        event1.context.pdata.model should be("DeviceUsageSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        
        val eks = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks.get("num_days").get should be(12)
        eks.get("start_time").get should be(1460627512768L)
        eks.get("avg_time").get should be(2.5)
        eks.get("avg_num_launches").get should be(0.25)
        eks.get("end_time").get should be(1461669647260L)
       
 
        val rdd3 = loadFile[MeasuredEvent]("src/test/resources/device-usage-summary/test_data_2.log");
        val rdd4 = DeviceUsageSummary.execute(rdd3, Option(Map("modelVersion" -> "1.0", "modelId" -> "DeviceUsageSummarizer","producerId" -> "AnalyticsDataPipeline")));
        val me2 = rdd4.collect()
        me2.length should be(2)
        
        val event2 = JSONUtils.deserialize[MeasuredEvent](me2(1));
        
        event2.eid should be("ME_DEVICE_USAGE_SUMMARY");
        event2.mid should be("0b303d4d66d13ad0944416780e52cc3db1feba87");
        event2.context.pdata.model should be("DeviceUsageSummarizer");
        event2.context.pdata.ver should be("1.0");
        event2.context.granularity should be("DAY");
        event2.context.date_range should not be null;
        
        val eks2 = event2.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks2.get("num_days").get should be(26)
        eks2.get("start_time").get should be(1460627512768L)
        eks2.get("avg_time").get should be(2.12)
        eks2.get("avg_num_launches").get should be(0.15)
        eks2.get("end_time").get should be(1462869647260L)
        
        val event3 = JSONUtils.deserialize[MeasuredEvent](me2(0));
        
        event3.eid should be("ME_DEVICE_USAGE_SUMMARY");
        event3.mid should be("267b8c9e00572f2ec4d3d463eb03f0f67d7f3636");
        event3.context.pdata.model should be("DeviceUsageSummarizer");
        event3.context.pdata.ver should be("1.0");
        event3.context.granularity should be("DAY");
        event3.context.date_range should not be null;
        
        val eks3 = event3.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks3.get("num_days").get should be(0)
        eks3.get("start_time").get should be(1460627674628L)
        eks3.get("avg_time").get should be(70.0)
        eks3.get("avg_num_launches").get should be(5.0)
        eks3.get("end_time").get should be(1460627674628L)
        
  }
}