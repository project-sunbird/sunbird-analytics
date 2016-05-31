package org.ekstep.analytics.model

import org.ekstep.analytics.framework.dispatcher.FileDispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.util.CommonUtil

class TestLearnerActivitySummary extends SparkSpec(null) {

    "LearnerActivitySummarizer" should "generate LearnerActivitySummarizer events from a sample file " in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/learner-activity-summary/learner_activity_test_sample.log");
        val rdd2 = LearnerActivitySummary.execute(rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerActivitySummary")));
        rdd2.collect().length should be(2)
    }

    it should "Print Learner Activity Summary events getting input from 'learner_activity_test_sample.log' and check the correctness" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/learner-activity-summary/learner_activity_test_sample.log");
        val rdd2 = LearnerActivitySummary.execute(rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerActivitySummary")));
        val me = rdd2.collect()
        me.length should be(2)

        val event1 = JSONUtils.deserialize[MeasuredEvent](me(0));
        val laSS1 = JSONUtils.deserialize[TimeSummary](JSONUtils.serialize(event1.edata.eks));

        laSS1.meanTimeSpent.get should be(81.15)
        laSS1.meanTimeBtwnGamePlays.get should be(16129.0)
        laSS1.start_ts should be(1456568148000L)
        laSS1.end_ts should be(1456762751000L)
        laSS1.last_visit_ts should be(1456762751000L)
        laSS1.meanInterruptTime.get should be(0d)
        laSS1.meanActiveTimeOnPlatform.get should be(81.15)
        laSS1.meanCountOfAct.get should be(Map("OTHER" -> 5.25, "LISTEN" -> 3.25, "END" -> 1.0, "TOUCH" -> 5.5))
        laSS1.meanTimeSpentOnAnAct should be(Map("OTHER" -> 6.0, "LISTEN" -> 1.0, "END" -> 21.6, "TOUCH" -> 145.5))
        laSS1.mostActiveHrOfTheDay.get should be(16)
        laSS1.numOfSessionsOnPlatform should be(13)
        laSS1.topKcontent.length should be(5)
        event1.mid should be ("E5CD001C35F87ABACE5E431476E9622C")
        event1.syncts should be (1456762355237L)

        val event2 = JSONUtils.deserialize[MeasuredEvent](me(1));
        val laSS2 = JSONUtils.deserialize[TimeSummary](JSONUtils.serialize(event2.edata.eks));
        laSS2.meanTimeSpent.get should be(83.13)
        laSS2.meanTimeBtwnGamePlays.get should be(1762.03)
        laSS2.start_ts should be(1456302608000L)
        laSS2.end_ts should be(1456384610000L)
        laSS2.last_visit_ts should be(1456384610000L)
        laSS2.meanInterruptTime.get should be(248.95d)
        laSS2.meanActiveTimeOnPlatform.get should be(83.13)
        laSS2.meanCountOfAct.get should be(Map("CHOOSE" -> 2.0, "OTHER" -> 14.38, "DRAG" -> 4.5, "LISTEN" -> 9.93, "END" -> 1.0, "DROP" -> 4.5, "TOUCH" -> 10.32))
        laSS2.meanTimeSpentOnAnAct should be(Map("CHOOSE" -> 21.0, "OTHER" -> 0.19, "DRAG" -> 30.0, "LISTEN" -> 5.93, "END" -> 3.5, "DROP" -> 5.25, "TOUCH" -> 81.0))
        laSS2.mostActiveHrOfTheDay.get should be(7)
        laSS2.numOfSessionsOnPlatform should be(40)
        laSS2.topKcontent.length should be(5)
        event2.mid should be ("8C3609B9F4B7EA660DA9F35743BD1350")
        event2.syncts should be (1456762355179L)
    }

    it should "check the correctness of the learner activity summary" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/learner-activity-summary/learner_activity_summary_sample1.log");
        val rdd2 = LearnerActivitySummary.execute(rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerActivitySummary")));
        val me = rdd2.collect()
        me.length should be(2)

        val event1 = JSONUtils.deserialize[MeasuredEvent](me(0));
        event1.eid should be("ME_LEARNER_ACTIVITY_SUMMARY");
        event1.context.pdata.model should be("LearnerActivitySummary");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("WEEK");
        event1.context.date_range should not be null;
        event1.uid should be("3b322350-393f-4eed-9564-0ed2ada90dba")
        event1.mid should be ("CDFE7849A27A1C429EDFB7F8422F9DE3")
        event1.syncts should be (1459488320133L)

        val laSS1 = JSONUtils.deserialize[TimeSummary](JSONUtils.serialize(event1.edata.eks));

        laSS1.meanTimeSpent.get should be(178.2)
        laSS1.meanTimeBtwnGamePlays.get should not be <(0)
        laSS1.start_ts should not be >=(laSS1.end_ts)
        laSS1.meanInterruptTime.get should be(0d)
        laSS1.meanActiveTimeOnPlatform.get should be(laSS1.meanTimeSpent.get - laSS1.meanInterruptTime.get)
        laSS1.meanCountOfAct.get should not be (null)
        laSS1.meanTimeSpentOnAnAct should not be (null)
        laSS1.mostActiveHrOfTheDay.get should be >= (0)
        laSS1.numOfSessionsOnPlatform should not be (0)
        laSS1.topKcontent.length should be(2)
    }

    it should "generate events with some special case in the input data (i.e missing activitySummary field, duplicate events / meanTimeBtwnGamePlays= -ve, etc..)" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/learner-activity-summary/learner_activity_test_sample1.log");
        val rdd2 = LearnerActivitySummary.execute(rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerActivitySummary", "topContent" -> Int.box(0))));
        rdd2.collect().length should be(2)
    }
    
    //Test cases for all the field in Learner Activity Summary
    it should "check all the fields, for timeSpent=0" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/learner-activity-summary/time_spent_zero.log");
        val rdd2 = LearnerActivitySummary.execute(rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerActivitySummary")));
        val me = rdd2.collect()
        me.length should be (1)
        
        val e1 = JSONUtils.deserialize[MeasuredEvent](me(0))
        val eks1 = JSONUtils.deserialize[TimeSummary](JSONUtils.serialize(e1.edata.eks));
        eks1.meanTimeSpent.get should be (0)
        eks1.meanActiveTimeOnPlatform.get should be (0)
        eks1.meanInterruptTime.get should be (0)
        eks1.totalTimeSpentOnPlatform.get should be (0)
        eks1.meanTimeSpentOnAnAct.size should be (0)
        eks1.meanCountOfAct.size should be (1)
    }
    
    it should "check the event fields where activitySummary is empty but timeSpent is non-zero" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/learner-activity-summary/empty_activity_summary_nonzero_timeSpent.log");
        val rdd2 = LearnerActivitySummary.execute(rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerActivitySummary")));
        val me = rdd2.collect()
        me.length should be (1)
        
        val e1 = JSONUtils.deserialize[MeasuredEvent](me(0))
        val eks1 = JSONUtils.deserialize[TimeSummary](JSONUtils.serialize(e1.edata.eks));
        
        eks1.meanTimeSpent.get should not be (0)
        eks1.meanActiveTimeOnPlatform.get should not be (0)
        eks1.meanInterruptTime.get should be (0)
        eks1.totalTimeSpentOnPlatform.get should not be (0)
        eks1.meanTimeSpentOnAnAct.size should be (0)
        eks1.meanCountOfAct.size should be (1)
    }
}