package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Dispatcher

class TestGenieLaunchSummaryModel extends SparkSpec(null) {

    "GenieLaunchSummaryModel" should "generate genie summary events" in {
        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/test-data1.log");
        val rdd2 = GenieLaunchSummaryModel.execute(rdd, None);
        val events = rdd2.collect
        events.size should be(73)
        
        // check the number of events where timeSpent==0 
        val zeroTimeSpentGE = events.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double] }.filter { x => 0 == x }
        zeroTimeSpentGE.size should be(10)

        val event1 = events.last
        val tags = event1.tags.get
        //tags.size should be (0)

        val event0 = events(0)

        val tags0 = event0.tags.get

        val eksMap1 = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap1.get("timeSpent").get.asInstanceOf[Double] should be(493.0)
        eksMap1.get("time_stamp").get.asInstanceOf[Long] should be(1461567474000l)
        eksMap1.get("contentCount").get.asInstanceOf[Int] should not be (0)
    }

    it should "generate the genie summary of the input data where some of the events generated after idle time" in {
        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/test-data2.log")
        val rdd2 = GenieLaunchSummaryModel.execute(rdd, None);
        val events = rdd2.collect
        events.size should be(7)

        val event1 = events.last
        event1.edata.eks.asInstanceOf[Map[String, AnyRef]].get("contentCount").get.asInstanceOf[Int] should be(0)
    }

    // test cases 
    it should "generate the genie summary from the input events with time difference less than idle time (30 mins)" in {
        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/test-data3.log")
        val rdd2 = GenieLaunchSummaryModel.execute(rdd, None);
        val events = rdd2.collect
        events.size should be(1)

        val event = events.last
        val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap.get("timeSpent").get.asInstanceOf[Double] should not be (0)
    }

    it should "generate the genie summary from the input events with time difference more than the idle time (30 mins)" in {
        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/test-data4.log")
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
        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/test-data5.log")
        val rdd2 = GenieLaunchSummaryModel.execute(rdd, None);

        val events = rdd2.collect
        events.size should be(2)

        val gseEvent1 = events(0)
        val gseEksMap1 = gseEvent1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        gseEksMap1.get("timeSpent").get.asInstanceOf[Double] should not be (0)

        val tags = gseEvent1.tags.get
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
        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/test-data6.log")
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

        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/test-data7.log")
        val rdd2 = GenieLaunchSummaryModel.execute(rdd, None);

        val events = rdd2.collect
        events.size should be(1)

        val gseEvent1 = events.last
        val gseEksMap1 = gseEvent1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        gseEksMap1.get("timeSpent").get.asInstanceOf[Double] should be > (0d)
        gseEksMap1.get("contentCount").get.asInstanceOf[Int] should be > (0)

    }

    it should "generate the genie summary from the input of (N + M) events of two session where the time diff between (x)th and (x + 1)th event is more than idle time (30 min), (where N-1 > x)" in {
        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/test-data8.log")
        val rdd2 = GenieLaunchSummaryModel.execute(rdd, None);

        val events = rdd2.collect
        events.size should be(2)

        val gseEvent1 = events(0)
        val gseEksMap1 = gseEvent1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        gseEksMap1.get("timeSpent").get.asInstanceOf[Double] should be > (0d)
        gseEksMap1.get("contentCount").get.asInstanceOf[Int] should be > (0)
    }
    
    it should "generate the genie summary with stage summary" in {
        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/stage/test-data1.log")
        val rdd2 = GenieLaunchSummaryModel.execute(rdd, None);
        val events = rdd2.collect
        events.size should be(3)
    }
}