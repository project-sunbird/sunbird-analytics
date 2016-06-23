package org.ekstep.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author Santhosh
 */
class TestSessionBatchModel extends SparkSpec {
  
    "SessionBatchModel" should "group data by game session" in {
        
        val rdd = SampleModel.execute(events, None);
        rdd.count should be (134);
        
    }
    it should "group data by device Id for GenieLaunchSummary" in {
        val events = loadFile[Event]("src/test/resources/session-batch-model/test-data-launch.log");
        val rdd = SampleModel.execute(events, Option(Map("model"->"GenieLaunch")));
        rdd.count should be (10);
    }
    
    it should "group data by session id for GenieSessionSummary" in {
        val events = loadFile[Event]("src/test/resources/session-batch-model/test-data-session.log");
        val rdd = SampleModel.execute(events, Option(Map("model"->"GenieSession")));
        rdd.count should be (8);
    }
}