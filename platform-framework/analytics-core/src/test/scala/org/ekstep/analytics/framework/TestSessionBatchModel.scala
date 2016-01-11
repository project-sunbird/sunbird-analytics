package org.ekstep.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object TestSessionBatchModel extends SessionBatchModel[Event] {
    def execute(sc: SparkContext, events: RDD[Event], jobParams: Option[Map[String, AnyRef]]) : RDD[String] = {
        val gameSessions = getGameSessions(events);
        gameSessions.map(f => f._1);
    }
}

/**
 * @author Santhosh
 */
class TestSessionBatchModel extends SparkSpec {
  
    "SessionBatchModel" should "group data by game session" in {
        
        val rdd = TestSessionBatchModel.execute(sc, events, None);
        rdd.count should be (380);
    }
}