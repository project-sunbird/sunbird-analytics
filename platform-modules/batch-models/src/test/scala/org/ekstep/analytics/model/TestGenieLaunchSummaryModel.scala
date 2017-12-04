package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.V3Event

class TestGenieLaunchSummaryModel extends SparkSpec(null) {

    "GenieLaunchSummaryModel" should "generate genie summary events" in {
        val rdd = loadFile[V3Event]("src/test/resources/genie-usage-summary/test-data1.log");
        val rdd2 = GenieLaunchSummaryModel.execute(rdd, None);
        val events = rdd2.collect
        events.size should be(72)
        
        // check the number of events where timeSpent==0 
        val zeroTimeSpentGE = events.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double] }.filter { x => 0 == x }
        zeroTimeSpentGE.size should be(13)
            
        val event1 = events.head
        val tags = event1.etags.get
        //tags.size should be (0)

        val event0 = events(0)
        println(JSONUtils.serialize(event1))
        val tags0 = event0.etags.get
        val eksMap1 = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap1.get("timeSpent").get.asInstanceOf[Double] should be(98.0)
        eksMap1.get("time_stamp").get.asInstanceOf[Number].longValue() should be(1457695757000l)
        eksMap1.get("contentCount").get.asInstanceOf[Int] should not be (0)
    }

    it should "generate the genie summary of the input data where some of the events generated after idle time" in {
        val rdd = loadFile[V3Event]("src/test/resources/genie-usage-summary/test-data2.log")
        val rdd2 = GenieLaunchSummaryModel.execute(rdd, None);
        val events = rdd2.collect
        events.size should be(7)

        val event1 = events.last
        event1.edata.eks.asInstanceOf[Map[String, AnyRef]].get("contentCount").get.asInstanceOf[Int] should be(0)
    }

    // test cases 
    it should "generate the genie summary from the input events with time difference less than idle time (30 mins)" in {
        val rdd = loadFile[V3Event]("src/test/resources/genie-usage-summary/test-data3.log")
        val rdd2 = GenieLaunchSummaryModel.execute(rdd, None);
        val events = rdd2.collect
        events.size should be(1)

        val event = events.last
        val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap.get("timeSpent").get.asInstanceOf[Double] should not be (0)
    }

    it should "generate the genie summary from the input events with time difference more than the idle time (30 mins)" in {
        val rdd = loadFile[V3Event]("src/test/resources/genie-usage-summary/test-data4.log")
        val rdd2 = GenieLaunchSummaryModel.execute(rdd, None);

        val events = rdd2.collect
        events.size should be(2)

        val event1 = events(0)
        val eksMap1 = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap1.get("timeSpent").get.asInstanceOf[Double] should be(0)

        val event2 = events.last
        val eksMap2 = event2.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap2.get("timeSpent").get.asInstanceOf[Double] should be(0)
    }

    it should "generate the genie summary from the input of ten events where the time diff between 9th and 10th event is more than idle time (30 mins)" in {
        val rdd = loadFile[V3Event]("src/test/resources/genie-usage-summary/test-data5.log")
        val rdd2 = GenieLaunchSummaryModel.execute(rdd, None);

        val events = rdd2.collect
        events.size should be(2)

        val gseEvent1 = events(0)
        val gseEksMap1 = gseEvent1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        gseEksMap1.get("timeSpent").get.asInstanceOf[Double] should not be (0)

        val tags = gseEvent1.etags.get
        /*
        tags.size should be (3)
        
        val tagMap0 = tags(0).asInstanceOf[Map[String,AnyRef]]
        tagMap0.size should be (1)
        tagMap0.get("survey_codes").get should be ("aser007")
        
        val tagMap = tags.last.asInstanceOf[Map[String,AnyRef]]
        tagMap.size should be (1)
        tagMap.get("partnerid").get should be ("org.ekstep.partner.pratham")
        */
        val gseEvent2 = events.last
        val gseEksMap2 = gseEvent2.edata.eks.asInstanceOf[Map[String, AnyRef]]
        gseEksMap2.get("timeSpent").get.asInstanceOf[Double] should be(0)
    }

    it should "generate the genie summary from the input of N events where the time diff between (x)th and (x+1)th event is more than idle time (30 mins), (where N-1 > x)" in {
        val rdd = loadFile[V3Event]("src/test/resources/genie-usage-summary/test-data6.log")
        val rdd2 = GenieLaunchSummaryModel.execute(rdd, None);

        val events = rdd2.collect
        events.size should be(2)

        val gseEvent1 = events(0)
        val gseEksMap1 = gseEvent1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        gseEksMap1.get("timeSpent").get.asInstanceOf[Double] should be > (0d)

        val gseEvent2 = events.last
        val gseEksMap2 = gseEvent2.edata.eks.asInstanceOf[Map[String, AnyRef]]
        gseEksMap2.get("timeSpent").get.asInstanceOf[Double] should be > (0d)
    }

    it should "generate the genie summary from the input of N events of multiple genie session where the time diff between each event is less than idle time (30 mins)" in {

        val rdd = loadFile[V3Event]("src/test/resources/genie-usage-summary/test-data7.log")
        val rdd2 = GenieLaunchSummaryModel.execute(rdd, None);

        val events = rdd2.collect
        events.size should be(1)

        val gseEvent1 = events.last
        val gseEksMap1 = gseEvent1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        gseEksMap1.get("timeSpent").get.asInstanceOf[Double] should be > (0d)
        gseEksMap1.get("contentCount").get.asInstanceOf[Int] should be > (0)

    }

    it should "generate the genie summary from the input of (N + M) events of two session where the time diff between (x)th and (x + 1)th event is more than idle time (30 min), (where N-1 > x)" in {
        val rdd = loadFile[V3Event]("src/test/resources/genie-usage-summary/test-data8.log")
        val rdd2 = GenieLaunchSummaryModel.execute(rdd, None);

        val events = rdd2.collect
        events.size should be(2)

        val gseEvent1 = events(0)
        val gseEksMap1 = gseEvent1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        gseEksMap1.get("timeSpent").get.asInstanceOf[Double] should be > (0d)
        gseEksMap1.get("contentCount").get.asInstanceOf[Int] should be > (0)
    }
    
    it should "generate the genie summary with stage summary having ideal time between events, all stages visited once and only one interact per stage " in {
        val rdd = loadFile[V3Event]("src/test/resources/genie-usage-summary/stage/test-data1.log")
        val rdd2 = GenieLaunchSummaryModel.execute(rdd, None);
        val events = rdd2.collect
        events.size should be(2)
        
        val eksMap1 = events(1).edata.eks.asInstanceOf[Map[String, AnyRef]]
        val screenSummary1 = eksMap1.get("screenSummary")
        val summaryList1 = JSONUtils.deserialize[List[GenieStageSummary]](JSONUtils.serialize(screenSummary1));
        
        summaryList1.size should be(0);
        
        val eksMap2 = events(0).edata.eks.asInstanceOf[Map[String, AnyRef]]
        val screenSummary2 = eksMap2.get("screenSummary")
        val summaryList2 = JSONUtils.deserialize[List[GenieStageSummary]](JSONUtils.serialize(screenSummary2));
        
        summaryList2.size should be(3);
        
        val summary2 = summaryList2(0)
        summary2.stageId should be("endStage");
        summary2.visitCount should be(1);
        summary2.interactEventsCount should be(0);
        summary2.timeSpent should be(5.656);
        
        val summary3 = summaryList2(1)
        summary3.stageId should be("Genie-Home-ChildContent-List");
        summary3.visitCount should be(1);
        summary3.interactEventsCount should be(3);
        summary3.timeSpent should be(1.532);
        
        val summary4 = summaryList2(2)
        summary4.stageId should be("splash");
        summary4.visitCount should be(1);
        summary4.interactEventsCount should be(0);
        summary4.timeSpent should be(0.0);
    }
    
    it should "generate the genie summary with stage summary having multiple visits" in {
        val rdd = loadFile[V3Event]("src/test/resources/genie-usage-summary/stage/test-data2.log")
        val rdd2 = GenieLaunchSummaryModel.execute(rdd, None);
        val events = rdd2.collect
        events.size should be(1)

        val eksMap1 = events(0).edata.eks.asInstanceOf[Map[String, AnyRef]]
        val summary = eksMap1.get("screenSummary")
        val summaryList = JSONUtils.deserialize[List[GenieStageSummary]](JSONUtils.serialize(summary));
        
        summaryList.size should be(4);
        
        val summary1 = summaryList(0)
        summary1.stageId should be("endStage");
        summary1.visitCount should be(1);
        summary1.interactEventsCount should be(0);
        summary1.timeSpent should be(4.615);
        
        val summary2 = summaryList(1)
        summary2.stageId should be("Genie-Home-ChildContent-List");
        summary2.visitCount should be(1);
        summary2.interactEventsCount should be(1);
        summary2.timeSpent should be(1.041);
        
        val summary3 = summaryList(2)
        summary3.stageId should be("Genie-Home-ChildContent-Search");
        summary3.visitCount should be(1);
        summary3.interactEventsCount should be(1);
        summary3.timeSpent should be(1.532);
        
        val summary4 = summaryList(3)
        summary4.stageId should be("splash");
        summary4.visitCount should be(1);
        summary4.interactEventsCount should be(0);
        summary4.timeSpent should be(0.0);
    }
}