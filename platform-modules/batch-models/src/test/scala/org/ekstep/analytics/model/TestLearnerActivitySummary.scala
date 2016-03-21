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

    "LearnerActivitySummarizer" should "generate LearnerActivitySummarizer events to a file" in {
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

        laSS1.meanTimeSpent.get should be(364.0d)
        laSS1.meanTimeBtwnGamePlays.get should be(43520.5)
        laSS1.start_ts should be(1450497687000L)
        laSS1.end_ts should be(1450585819000L)
        laSS1.last_visit_ts should be(1450585819000L)
        laSS1.meanInterruptTime.get should be(0d)
        laSS1.meanActiveTimeOnPlatform.get should be(364.0d - 0d)
        laSS1.meanCountOfAct.get should be(Map("TOUCH" -> 13.5))
        laSS1.meanTimeSpentOnAnAct should be(Map("TOUCH" -> 91.5))
        laSS1.mostActiveHrOfTheDay.get should be(4)
        laSS1.numOfSessionsOnPlatform should be(3)
        laSS1.topKcontent.length should be(1)
        event1.mid should be ("807C892AA2A66840DACE1870C0C18AD4")
        event1.syncts should be (1452080546615L)

        val event2 = JSONUtils.deserialize[MeasuredEvent](me(1));
        val laSS2 = JSONUtils.deserialize[TimeSummary](JSONUtils.serialize(event2.edata.eks));
        laSS2.meanTimeSpent.get should be(4d)
        laSS2.meanTimeBtwnGamePlays.get should be(0d)
        laSS2.start_ts should be(1450508862000L)
        laSS2.end_ts should be(1450508866000L)
        laSS2.last_visit_ts should be(1450508866000L)
        laSS2.meanInterruptTime.get should be(0d)
        laSS2.meanActiveTimeOnPlatform.get should be(4d)
        laSS2.meanCountOfAct.get should be(empty)
        laSS2.meanTimeSpentOnAnAct should be(empty)
        laSS2.mostActiveHrOfTheDay.get should be(7)
        laSS2.numOfSessionsOnPlatform should be(1)
        laSS2.topKcontent.length should be(1)
        event2.mid should be ("48A8FFC32DAAE1607916D245F23683B5")
        event2.syncts should be (1452080546620L)
    }

    it should "check the correctness of the learner activity summary" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/learner-activity-summary/learner_activity_summary_sample1.log");
        val rdd2 = LearnerActivitySummary.execute(rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerActivitySummary")));
        val me = rdd2.collect()
        me.length should be(1)

        val event1 = JSONUtils.deserialize[MeasuredEvent](me(0));
        event1.eid should be("ME_LEARNER_ACTIVITY_SUMMARY");
        event1.context.pdata.model should be("LearnerActivitySummary");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("WEEK");
        event1.context.date_range should not be null;
        event1.uid.get should be("62e0fb81-f7e5-4e03-8ca0-fe67764b0039")
        event1.mid should be ("FE0C3870E68D97198FF67AE5BAAEB390")
        event1.syncts should be (1452080546895L)

        val laSS1 = JSONUtils.deserialize[TimeSummary](JSONUtils.serialize(event1.edata.eks));

        laSS1.meanTimeSpent.get should be(451)
        laSS1.meanTimeBtwnGamePlays.get should not be <(0)
        laSS1.start_ts should not be >=(laSS1.end_ts)
        laSS1.meanInterruptTime.get should be(0d)
        laSS1.meanActiveTimeOnPlatform.get should be(laSS1.meanTimeSpent.get - laSS1.meanInterruptTime.get)
        laSS1.meanCountOfAct.get should not be (null)
        laSS1.meanTimeSpentOnAnAct should not be (null)
        laSS1.mostActiveHrOfTheDay.get should be >= (0)
        laSS1.numOfSessionsOnPlatform should not be (0)
        laSS1.topKcontent.length should be(1)
    }

    it should " generate events with some special case in the input data (i.e missing activitySummary field, duplicate events / meanTimeBtwnGamePlays= -ve, etc..)" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/learner-activity-summary/learner_activity_test_sample1.log");
        val rdd2 = LearnerActivitySummary.execute(rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerActivitySummary", "topContent" -> Int.box(0))));
        rdd2.collect().length should be(2)
    }
    
    //Test cases for all the field in Learner Activity Summary
    it should " check all the fields, for timeSpent=0" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/learner-activity-summary/time_spent_zero.log");
        val rdd2 = LearnerActivitySummary.execute(rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerActivitySummary")));
        val me = rdd2.collect()
        me.length should be (2)
        
        val e1 = JSONUtils.deserialize[MeasuredEvent](me(0))
        val eksMap1 = e1.edata.eks.asInstanceOf[Map[String,AnyRef]];
        eksMap1.get("meanTimeSpent").get should be (0)
        eksMap1.get("meanActiveTimeOnPlatform").get should be (0)
        eksMap1.get("meanInterruptTime").get should be (0)
        eksMap1.get("totalTimeSpentOnPlatform").get should be (0)
        eksMap1.get("meanTimeSpentOnAnAct").get.asInstanceOf[Map[String,Double]].size should be (0)
        eksMap1.get("meanCountOfAct").get.asInstanceOf[Map[String,Double]].size should be (0)
        
        val e2 = JSONUtils.deserialize[MeasuredEvent](me(1))
        val eksMap2 = e2.edata.eks.asInstanceOf[Map[String,AnyRef]];
        eksMap2.get("meanTimeSpent").get should be (0)
        eksMap2.get("meanActiveTimeOnPlatform").get should be (0)
        eksMap2.get("meanInterruptTime").get should be (0)
        eksMap2.get("totalTimeSpentOnPlatform").get should be (0)
        eksMap2.get("meanTimeSpentOnAnAct").get.asInstanceOf[Map[String,Double]].size should be (0)
        eksMap2.get("meanCountOfAct").get.asInstanceOf[Map[String,Double]].size should be (0)
    }
    
    it should " check the event fields where activitySummary is empty but timeSpent is non-zero" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/learner-activity-summary/empty_activity_summary_nonzero_timeSpent.log");
        val rdd2 = LearnerActivitySummary.execute(rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerActivitySummary")));
        val me = rdd2.collect()
        me.length should be (2)
        
        val e1 = JSONUtils.deserialize[MeasuredEvent](me(0))
        val eksMap1 = e1.edata.eks.asInstanceOf[Map[String,AnyRef]];
        eksMap1.get("meanTimeSpent").get should not be (0)
        eksMap1.get("meanActiveTimeOnPlatform").get should not be (0)
        eksMap1.get("meanInterruptTime").get should be (0)
        eksMap1.get("totalTimeSpentOnPlatform").get should not be (0)
        eksMap1.get("meanTimeSpentOnAnAct").get.asInstanceOf[Map[String,Double]].size should be (0)
        eksMap1.get("meanCountOfAct").get.asInstanceOf[Map[String,Double]].size should be (0)
        
        val e2 = JSONUtils.deserialize[MeasuredEvent](me(1))
        val eksMap2 = e2.edata.eks.asInstanceOf[Map[String,AnyRef]];
        eksMap2.get("meanTimeSpent").get should not be (0)
        eksMap2.get("meanActiveTimeOnPlatform").get should not be (0)
        eksMap2.get("meanInterruptTime").get should be (0)
        eksMap2.get("meanTimeSpentOnAnAct").get.asInstanceOf[Map[String,Double]].size should be (0)
        eksMap2.get("meanCountOfAct").get.asInstanceOf[Map[String,Double]].size should be (0)
    }
}