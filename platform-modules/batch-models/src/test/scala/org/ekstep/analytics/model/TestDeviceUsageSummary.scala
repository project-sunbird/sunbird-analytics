package org.ekstep.analytics.model

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.util.JSONUtils

class TestDeviceUsageSummary extends SparkSpec(null) {
  
  "DeviceUsageSummary" should "generate DeviceUsageSummary events from a sample file " in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/device-usage-summary/test_data_1.log");
        val rdd2 = DeviceUsageSummary.execute(rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "DeviceUsageSummarizer","granularity" -> "DAY")));
        val me = rdd2.collect()
        println(me(0))
        
        me.length should be(1)
        
        val event1 = JSONUtils.deserialize[MeasuredEvent](me(0));
        
        event1.eid should be("ME_DEVICE_USAGE_SUMMARY");
        event1.mid should be("0b303d4d66d13ad0944416780e52cc3db1feba87");
        event1.context.pdata.model should be("DeviceUsageSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        
        val eks = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks.get("num_days").get should be(13)
        eks.get("start_time").get should be(1460627512768L)
        eks.get("avg_time").get should be(2.31)
        eks.get("avg_num_launches").get should be(0.23)
        eks.get("end_time").get should be(1461669647260L)
       
  }
  
  it should "generate 2 DeviceUsageSummary events from a sample file " in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/device-usage-summary/test_data_2.log");
        val rdd2 = DeviceUsageSummary.execute(rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "DeviceUsageSummarizer","producerId" -> "AnalyticsDataPipeline")));
        val me = rdd2.collect()
        println(me(0))
        println(me(1))
        me.length should be(2)
        
        val event1 = JSONUtils.deserialize[MeasuredEvent](me(1));
        
        event1.eid should be("ME_DEVICE_USAGE_SUMMARY");
        event1.mid should be("0b303d4d66d13ad0944416780e52cc3db1feba87");
        event1.context.pdata.model should be("DeviceUsageSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        
        val eks = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks.get("num_days").get should be(27)
        eks.get("start_time").get should be(1460627512768L)
        eks.get("avg_time").get should be(2.04)
        eks.get("avg_num_launches").get should be(0.15)
        eks.get("end_time").get should be(1462869647260L)
        
  }
}