package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.DerivedEvent

class TestGenieStageSummaryModel extends SparkSpec(null) {
  
    "GenieStageSummaryModel" should "generate genie stage summary in case of ideal time between events" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/genie-stage-summary/test-data1.log");
        val rdd2 = GenieStageSummaryModel.execute(rdd, None);
        val events = rdd2.collect
        events.size should be(4)
        
        //genie stage summary where only one interact per stage
        val event1 = events(3)
        val eksMap1 = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap1.get("interactEventsCount").get.asInstanceOf[Int] should be(1)
        eksMap1.get("stageVisitCount").get.asInstanceOf[Int] should be (1)
        
        //genie stage summary in case of stage visited once
        val event2 = events(0)
        val eksMap2 = event2.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap2.get("timeSpent").get.asInstanceOf[Double] should not be(0.0)
        eksMap2.get("interactEventsCount").get.asInstanceOf[Int] should be >= (0)
        eksMap2.get("stageVisitCount").get.asInstanceOf[Int] should be (1)
        
    }
    
    it should "generate genie stage summary where all the screens are visited multiple times" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/genie-stage-summary/test-data2.log");
        val rdd2 = GenieStageSummaryModel.execute(rdd, None);
        val events = rdd2.collect
        events.size should not be(0)
        
        val event1 = events(1)
        val eksMap1 = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap1.get("timeSpent").get.asInstanceOf[Double] should not be(0.0)
        eksMap1.get("interactEventsCount").get.asInstanceOf[Int] should be > (0)
        eksMap1.get("stageVisitCount").get.asInstanceOf[Int] should be > (1)
    }
}