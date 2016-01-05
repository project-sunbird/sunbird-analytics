package org.ekstep.analytics.model

import org.ekstep.analytics.framework.SparkSpec
import java.io.FileWriter
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher

/**
 * @author Santhosh
 */
class TestGenericSessionSummary extends SparkSpec(null) {
    
    "GenericScreenerSummary" should "generate session summary and pass all positive test cases" in {
        
        val rdd = loadFile("src/test/resources/session-summary/test_data1.log");
        val screener = new GenericSessionSummary();
        val rdd2 = screener.execute(sc, rdd, Option(Map("contentId" -> "numeracy_382", "modelVersion" -> "1.4", "modelId" -> "GenericSessionSummary")));
        val me = rdd2.collect();
        me.length should be (1);
        val event1 = JSONUtils.deserialize[MeasuredEvent](me(0));
        event1.eid should be ("ME_SESSION_SUMMARY");
        event1.context.pdata.model should be ("GenericSessionSummary");
        event1.context.pdata.ver should be ("1.4");
        event1.context.granularity should be ("SESSION");
        event1.context.dt_range should not be null;
        event1.dimensions.gdata.get.id should be ("org.ekstep.aser.lite");
        
        val summary1 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event1.edata.eks));
        summary1.noOfLevelTransitions.get should be (1);
        summary1.levels should not be (None);
        summary1.levels.get.length should be (2);
        summary1.noOfAttempts should be (2);
        summary1.timeSpent should be (876);
        summary1.interactEventsPerMin should be (2.74);
        summary1.currentLevel should not be (None);
        summary1.currentLevel.get.get("numeracy").get should be ("Can do subtraction");
        summary1.currentLevel.get.get("literacy").get should be ("Can read story");
        summary1.noOfInteractEvents should be (40);
        summary1.itemResponses.get.length should be (5);
        summary1.activitySummary.get.size should be (2);
        summary1.activitySummary.get.get("TOUCH").get.count should be (31);
        summary1.activitySummary.get.get("TOUCH").get.timeSpent should be (757);
        summary1.activitySummary.get.get("DRAG").get.count should be (9);
        summary1.activitySummary.get.get("DRAG").get.timeSpent should be (115);
    }
    
    it should "generate 4 session summarries and pass all negative test cases" in {
        
        val rdd = loadFile("src/test/resources/session-summary/test_data2.log");
        val screener = new GenericSessionSummary();
        val rdd2 = screener.execute(sc, rdd, Option(Map("contentId" -> "numeracy_382", "modelVersion" -> "1.2", "modelId" -> "GenericContentSummary")));
        val me = rdd2.collect();
        me.length should be (4);
        
        val event1 = JSONUtils.deserialize[MeasuredEvent](me(0));
        // Validate for event envolope
        event1.eid should be ("ME_SESSION_SUMMARY");
        event1.context.pdata.model should be ("GenericContentSummary");
        event1.context.pdata.ver should be ("1.2");
        event1.context.granularity should be ("SESSION");
        event1.context.dt_range should not be null;
        event1.dimensions.gdata.get.id should be ("org.ekstep.aser.lite");
        
        val summary1 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event1.edata.eks));
        summary1.noOfLevelTransitions.get should be (-1);
        summary1.levels should not be (None);
        summary1.levels.get.length should be (0);
        summary1.noOfAttempts should be (1);
        summary1.timeSpent should be (47);
        summary1.interactEventsPerMin should be (6.38);
        summary1.currentLevel should not be (None);
        summary1.currentLevel.get.size should be (0);
        summary1.noOfInteractEvents should be (5);
        summary1.itemResponses.get.length should be (0);
        summary1.activitySummary.get.size should be (1);
        summary1.activitySummary.get.get("TOUCH").get.count should be (5);
        summary1.activitySummary.get.get("TOUCH").get.timeSpent should be (47);
        
        val event2 = JSONUtils.deserialize[MeasuredEvent](me(1));
        val summary2 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event2.edata.eks));
        summary2.noOfLevelTransitions.get should be (0);
        summary2.levels should not be (None);
        summary2.levels.get.length should be (1);
        summary2.noOfAttempts should be (1);
        summary2.timeSpent should be (875);
        summary2.interactEventsPerMin should be (1.71);
        summary2.currentLevel should not be (None);
        summary2.currentLevel.get.size should be (1);
        summary2.currentLevel.get.get("literacy").get should be ("Can read story");
        summary2.noOfInteractEvents should be (25);
        summary2.itemResponses.get.length should be (2);
        summary2.activitySummary.get.size should be (1);
        summary2.activitySummary.get.get("TOUCH").get.count should be (25);
        summary2.activitySummary.get.get("TOUCH").get.timeSpent should be (739);
        
        val event3 = JSONUtils.deserialize[MeasuredEvent](me(2));
        val summary3 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event3.edata.eks));
        summary3.noOfLevelTransitions.get should be (-1);
        summary3.levels should not be (None);
        summary3.levels.get.length should be (0);
        summary3.noOfAttempts should be (1);
        summary3.timeSpent should be (0);
        summary3.interactEventsPerMin should be (0);
        summary3.currentLevel should not be (None);
        summary3.currentLevel.get.size should be (0);
        summary3.noOfInteractEvents should be (0);
        summary3.itemResponses.get.length should be (0);
        summary3.activitySummary.get.size should be (0);
        
        val event4 = JSONUtils.deserialize[MeasuredEvent](me(3));
        val summary4 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event4.edata.eks));
        summary4.noOfLevelTransitions.get should be (-1);
        summary4.levels should not be (None);
        summary4.levels.get.length should be (0);
        summary4.noOfAttempts should be (1);
        summary4.timeSpent should be (11);
        summary4.interactEventsPerMin should be (16.36);
        summary4.currentLevel should not be (None);
        summary4.currentLevel.get.size should be (0);
        summary4.noOfInteractEvents should be (3);
        summary4.itemResponses.get.length should be (0);
        summary4.activitySummary.get.size should be (1);
        summary4.activitySummary.get.get("TOUCH").get.count should be (3);
        summary4.activitySummary.get.get("TOUCH").get.timeSpent should be (11);
    }
}