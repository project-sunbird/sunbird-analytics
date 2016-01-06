package org.ekstep.analytics.model

import org.ekstep.analytics.framework.SparkSpec
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

class TestLearnerActivitySummarizer extends SparkSpec(null) {

    "LearnerActivitySummarizer" should "generate LearnerActivitySummarizer events to a file" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/session-summary/learner_activity_test_sample.log");
        val activitySummary = new LearnerActivitySummarizer();
        val rdd2 = activitySummary.execute(sc, rdd.map { x => x.asInstanceOf[AnyRef] }, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerActivitySummary")));
        val me = rdd2.collect()
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "src/test/resources/learner_activity_test_output.log")), rdd2);
    }
   
    it should "Print Learner Activity Summary events and check the correctness" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/session-summary/learner_activity_test_sample.log");
        val activitySummary = new LearnerActivitySummarizer();
        val rdd2 = activitySummary.execute(sc, rdd.map { x => x.asInstanceOf[AnyRef] }, Option(Map("modelVersion" -> "1.0", "modelId" -> "LearnerActivitySummary")));
        val me = rdd2.collect()
        me.length should be (2)
        
        val laSS1 = JSONUtils.deserialize[TimeSummary](JSONUtils.serialize(JSONUtils.deserialize[MeasuredEvent](me(0)).edata.eks));
        laSS1.meanTimeSpent.get should be (364.0d)
        //laSS1.meanTimeBtwnGamePlays.get should be (43520.5d) 
        laSS1.startTimestamp should be (1450497687000L)
        laSS1.endTimestamp should be (1450585819000L)
        laSS1.lastVisitTimeStamp should be (1450585819000L)
        laSS1.meanInterruptTime.get should be (0d)
        laSS1.meanActiveTimeOnPlatform.get should be (364.0d-0d)
        laSS1.meanCountOfAct.get should be (Map("TOUCH"->13.5)) 
        laSS1.meanTimeSpentOnAnAct should be (Map("TOUCH"->91.5))
        laSS1.mostActiveHrOfTheDay should be (10)
        laSS1.numOfSessionsOnPlatform should be (3)
        laSS1.topKcontent.length should be (1)
        
        val laSS2 = JSONUtils.deserialize[TimeSummary](JSONUtils.serialize(JSONUtils.deserialize[MeasuredEvent](me(1)).edata.eks));
        laSS2.meanTimeSpent.get should be (4d)
        laSS2.meanTimeBtwnGamePlays.get should be (0d) 
        laSS2.startTimestamp should be (1450508862000L)
        laSS2.endTimestamp should be (1450508866000L)
        laSS2.lastVisitTimeStamp should be (1450508866000L)
        laSS2.meanInterruptTime.get should be (0d)
        laSS2.meanActiveTimeOnPlatform.get should be (4d)
        laSS2.meanCountOfAct.get should be (empty) 
        laSS2.meanTimeSpentOnAnAct should be (empty)
        laSS2.mostActiveHrOfTheDay should be (12)
        laSS2.numOfSessionsOnPlatform should be (1)
        laSS2.topKcontent.length should be (1)
        
        OutputDispatcher.dispatch(Dispatcher("Console", Map("file" -> "src/test/resources/test_output.log")), rdd2);
    }
}