package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.GData
import org.ekstep.analytics.framework.MEEdata
import scala.collection.immutable.HashMap.HashTrieMap
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.DtRange

case class TimeSummary(meanTimeSpent: Option[Double], meanTimeBtwnGamePlays: Option[Double], meanActiveTimeOnPlatform: Option[Double], meanInterruptTime: Option[Double], totalTimeSpentOnPlatform: Option[Double], meanTimeSpentOnAnAct: Option[Double], numOfSessionsOnPlatform: Long, lastVisitTimeStamp: Long, mostActiveHrOfTheDay: Int);

/**
 * Case class to hold the screener summary
 */

class LearnerActivitySummarizer {
    def execute(sc: SparkContext, events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        val activity = events.map(event => (event.uid.get, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { x =>

                val startTimestamp = if (x.length > 0) { Option(x(0).ts) } else { Option(0l) };
                val endTimestamp = if (x.length > 0) { Option(x.last.ts) } else { Option(0l) };

                val numOfSessionsOnPlatform = x.length.toLong
                val timeSpentList = Buffer[Double]();
                val btwnGamePlays = Buffer[Double]();
                var lastGame: Long = 0l;
                var actCount = 0;
                var totalTimeSpentOnAct = 0d;
                var numOfAct = Set[String]();

                val sortedWndTime = x.map(x => (x.edata.eks.asInstanceOf[SessionSummary].endTimestamp.get, x.gdata.get.id, x.edata.eks.asInstanceOf[SessionSummary])) sortBy (f => f._1)
                val lastVisitTimeStamp = sortedWndTime.last._1

                x.foreach { y =>
                    var ss = y.edata.eks.asInstanceOf[SessionSummary];
                    timeSpentList += ss.timeSpent.get
                    if (lastGame == 0) { lastGame = ss.endTimestamp.get } else { btwnGamePlays += CommonUtil.getTimeDiff(lastGame, ss.startTimestamp.get).get; lastGame = 0l; }
                    ss.activitySummary.get.values.foreach { x => actCount += x.count; totalTimeSpentOnAct += x.timeSpent; }
                    numOfAct ++= ss.activitySummary.get.keySet
                }

//                val hrMax = x.groupBy { x =>
//                    var ss = x.edata.eks.asInstanceOf[SessionSummary]
//                    List(CommonUtil.getHourOfDay(ss.startTimestamp.get).get to CommonUtil.getHourOfDay(ss.endTimestamp.get).get)
//                }

                val meanTimeSpentOnAnAct = totalTimeSpentOnAct / numOfAct.size
                val meanCountOfAct = actCount / numOfAct.size
                val totalTimeSpentOnPlatform = timeSpentList.sum
                val meanTimeSpent = timeSpentList.sum / timeSpentList.length
                val meanTimeBtwnGamePlays = btwnGamePlays.sum / btwnGamePlays.length
                val topKcontent = sortedWndTime.map(f => f._2.distinct).take(5)
                val meanActiveTimeOnPlatform = meanTimeSpent;
                (TimeSummary(Option(meanTimeSpent), Option(meanTimeBtwnGamePlays), Option(meanActiveTimeOnPlatform), Option(0d), Option(totalTimeSpentOnPlatform), Option(meanTimeSpentOnAnAct), numOfSessionsOnPlatform, lastVisitTimeStamp, 0), DtRange(startTimestamp.getOrElse(0l), endTimestamp.getOrElse(0l)));
            }
        activity.map(f => {
            getMeasuredEvent(f, configMapping.value);
        }).map { x => JSONUtils.serialize(x) };
    }
    private def getMeasuredEvent(userMap: (String, (TimeSummary, DtRange)), config: Map[String, AnyRef]): MeasuredEvent = {
        val measures = userMap._2;

        MeasuredEvent(config.getOrElse("eventId", "ME_LEARNER_ACTIVITY_SUMMARY").asInstanceOf[String], System.currentTimeMillis(), "1.0", Option(userMap._1), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "LearnerActivitySummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "WEEK", userMap._2._2),
            Dimensions(None, None, None, None, None, None),
            MEEdata(measures));
    }
}