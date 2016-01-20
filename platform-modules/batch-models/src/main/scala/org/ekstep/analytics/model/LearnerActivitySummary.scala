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
import scala.collection.mutable.ListBuffer
import org.ekstep.analytics.framework.SessionBatchModel
import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.MeasuredEvent


/**
 * @author Amit Behera
 */
case class TimeSummary(meanTimeSpent: Option[Double], meanTimeBtwnGamePlays: Option[Double], meanActiveTimeOnPlatform: Option[Double], meanInterruptTime: Option[Double], totalTimeSpentOnPlatform: Option[Double], meanTimeSpentOnAnAct: Map[String, Double], meanCountOfAct: Option[Map[String, Double]], numOfSessionsOnPlatform: Long, lastVisitTimeStamp: Long, mostActiveHrOfTheDay: Option[Int], topKcontent: Array[String],startTimestamp: Long, endTimestamp: Long);

object LearnerActivitySummary extends IBatchModel[MeasuredEvent] with Serializable {

    def execute(sc: SparkContext, events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        val activity = events.map(event => (event.uid.get, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { x =>

                val sortedEvents = x.sortBy { x => x.ets };
                val eventStartTimestamp = sortedEvents(0).ets;
                val eventEndTimestamp = sortedEvents.last.ets;
                val startTimestamp = sortedEvents.map { x => x.context.dt_range.from }.sortBy { x => x }.toBuffer(0);
                val sortedGames = sortedEvents.sortBy(-_.context.dt_range.to).map(f => f.dimensions.gdata.get.id).distinct;
                val endTimestamp = sortedEvents.map { x => x.context.dt_range.to }.sortBy { x => x }.toBuffer.last;
                val summaryEvents = sortedEvents.map { x => x.edata.eks }.map { x => x.asInstanceOf[Map[String, AnyRef]] };
                val numOfSessionsOnPlatform = x.length;

                val lastVisitTimeStamp = endTimestamp;

                // Compute mean count and time spent of interact events grouped by type
                val interactSummaries = summaryEvents.map { x => x.get("activitySummary").get.asInstanceOf[Map[String, Map[String, AnyRef]]] }.filter(x => x.nonEmpty).flatMap(f => f.map { x => x }).map(f => (f._1, (f._2.getOrElse("count", 0).asInstanceOf[Int], f._2.getOrElse("timeSpent", 0d).asInstanceOf[Double])));
                val meanInteractSummaries = interactSummaries.groupBy(f => f._1).map(f => {
                    (f._1, average(f._2.map(f => f._2._1)), average(f._2.map(f => f._2._2)))
                })
                val meanTimeSpentOnAnAct = meanInteractSummaries.map(f => (f._1, f._3)).toMap;
                val meanCountOfAct = meanInteractSummaries.map(f => (f._1, f._2)).toMap;

                val meanTimeSpent = average(summaryEvents.map { x => x.getOrElse("timeSpent", 0d).asInstanceOf[Double] });
                val meanInterruptTime = average(summaryEvents.map { x => x.getOrElse("interruptTime", 0d).asInstanceOf[Double] });

                val totalTimeSpentOnPlatform = summaryEvents.map { x => x.getOrElse("timeSpent", 0d).asInstanceOf[Double] }.reduce((a, b) => a + b);
                val topKcontent = if (sortedGames.length > 5) sortedGames.take(5).toArray else sortedGames.toArray;
                val meanActiveTimeOnPlatform = meanTimeSpent - meanInterruptTime;
                val activeHours = summaryEvents.map { f =>
                    try {
                        (CommonUtil.getHourOfDay(f.getOrElse("startTime", 0l).asInstanceOf[Long], f.getOrElse("endTime", 0l).asInstanceOf[Long]))
                    } catch {
                        case ex: ClassCastException =>
                            null;
                    }
                }.filter(_ != null).flatten.map { x => (x, 1) }.groupBy(_._1).map(x => (x._1, x._2.length));
                
                val mostActiveHrOfTheDay = if(activeHours.isEmpty) None else Option(activeHours.maxBy(f => f._2)._1);
                
                val meanTimeBtwnGamePlays = if(summaryEvents.length>1)(CommonUtil.getTimeDiff(startTimestamp, endTimestamp).get - totalTimeSpentOnPlatform)/(summaryEvents.length-1)else 0d 

                (TimeSummary(Option(meanTimeSpent), Option(meanTimeBtwnGamePlays), Option(meanActiveTimeOnPlatform), Option(meanInterruptTime), Option(totalTimeSpentOnPlatform), meanTimeSpentOnAnAct, Option(meanCountOfAct), numOfSessionsOnPlatform, lastVisitTimeStamp, mostActiveHrOfTheDay, topKcontent ,startTimestamp, endTimestamp), DtRange(eventStartTimestamp, eventEndTimestamp));
            }
        activity.map(f => {
            getMeasuredEvent(f, configMapping.value);
        }).map { x => JSONUtils.serialize(x) };
    }

    private def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
        num.toDouble(ts.sum) / ts.size
    }

    private def getMeasuredEvent(userMap: (String, (TimeSummary, DtRange)), config: Map[String, AnyRef]): MeasuredEvent = {
        val measures = userMap._2._1;

        MeasuredEvent(config.getOrElse("eventId", "ME_LEARNER_ACTIVITY_SUMMARY").asInstanceOf[String], System.currentTimeMillis(), "1.0", Option(userMap._1), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "LearnerActivitySummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "WEEK", userMap._2._2),
            Dimensions(None, None, None, None, None, None),
            MEEdata(measures));
    }
}