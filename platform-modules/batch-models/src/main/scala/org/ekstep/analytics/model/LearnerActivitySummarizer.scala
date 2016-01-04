package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.MeasuredEvent

case class TimeSummary(window: Long, meanTimeSpent: Option[Double],meanTimeBtwnGamePlays: Option[Double],meanActiveTimeOnPlatform: Option[Double],meanInterruptTime: Option[Double],totalTimeSpentOnPlatform: Option[Double],meanTimeSpentOnAnAct: Option[AnyRef], numOfSessionsOnPlatform: Long, lastVisitTimeStamp: Long, mostActiveHrOfTheDay: Int);

/**
 * Case class to hold the screener summary
 */
//case class ActivitySummary(id: Option[String], ver: Option[String], levels: Option[Array[Map[String, Any]]], noOfAttempts: Int, timeSpent: Option[Double], startTimestamp: Option[Long], endTimestamp: Option[Long], currentLevel: Option[Map[String, String]], noOfLevelTransitions: Option[Int], comments: Option[String], fluency: Option[Int], loc: Option[String], itemResponses: Option[Buffer[TimeSummary]], dtRange: DtRange);

class LearnerActivitySummarizer {
    def execute(sc: SparkContext, events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);
        null;
    }
}