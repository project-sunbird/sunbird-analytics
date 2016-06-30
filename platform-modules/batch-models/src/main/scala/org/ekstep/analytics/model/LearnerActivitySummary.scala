package org.ekstep.analytics.model

import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.joda.time.DateTime
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.log4j.Logger

/**
 * @author Amit Behera
 */
case class TimeSummary(uid: String, dtRange: DtRange, meanTimeSpent: Option[Double], meanTimeBtwnGamePlays: Option[Double],
                       meanActiveTimeOnPlatform: Option[Double], meanInterruptTime: Option[Double], totalTimeSpentOnPlatform: Option[Double],
                       meanTimeSpentOnAnAct: Map[String, Double], meanCountOfAct: Option[Map[String, Double]], numOfSessionsOnPlatform: Long,
                       last_visit_ts: Long, mostActiveHrOfTheDay: Option[Int], topKcontent: Array[String], start_ts: Long, end_ts: Long) extends AlgoOutput
case class LearnerActivityInput(uid: String, events: Buffer[DerivedEvent]) extends AlgoInput

object LearnerActivitySummary extends IBatchModelTemplate[DerivedEvent, LearnerActivityInput, TimeSummary, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.LearnerActivitySummary"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[LearnerActivityInput] = {
        val filteredData = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")));

        JobLogger.debug("Calculating all activities per learner", className)
        val activity = filteredData.map(event => (event.uid, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b)
        activity.map { x => LearnerActivityInput(x._1, x._2) }
    }

    override def algorithm(data: RDD[LearnerActivityInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[TimeSummary] = {
        val configMapping = sc.broadcast(config);
        val topK = configMapping.value.getOrElse("topContent", 5).asInstanceOf[Int];
        data.map { x =>

            val sortedEvents = x.events.sortBy { x => x.syncts };
            val eventStartTimestamp = sortedEvents(0).syncts;
            val eventEndTimestamp = sortedEvents.last.syncts;
            val startTimestamp = sortedEvents.map { x => x.context.date_range.from }.sortBy { x => x }.toBuffer(0);
            val sortedGames = sortedEvents.sortBy(-_.context.date_range.to).map(f => f.dimensions.gdata.get.id).distinct;
            val endTimestamp = sortedEvents.map { x => x.context.date_range.to }.sortBy { x => x }.toBuffer.last;
            val summaryEvents = sortedEvents.map { x => x.edata.eks }.map { x => x.asInstanceOf[Map[String, AnyRef]] };
            val numOfSessionsOnPlatform = x.events.length;

            val lastVisitTimeStamp = endTimestamp;

            // Compute mean count and time spent of interact events grouped by type
            val interactSummaries = summaryEvents.map { x => x.getOrElse("activitySummary", List()).asInstanceOf[List[Map[String, AnyRef]]] }.filter(x => x.nonEmpty).flatMap(f => f.map { x => x }).map(f => (f.get("actType").get.asInstanceOf[String], (f.get("count").get.asInstanceOf[Int], f.get("timeSpent").get.asInstanceOf[Double])));
            val meanInteractSummaries = interactSummaries.groupBy(f => f._1).map(f => {
                (f._1, average(f._2.map(f => f._2._1)), average(f._2.map(f => f._2._2)))
            })
            val meanTimeSpentOnAnAct = meanInteractSummaries.map(f => (f._1, f._3)).toMap;
            val meanCountOfAct = meanInteractSummaries.map(f => (f._1, f._2)).toMap;

            val meanTimeSpent = average(summaryEvents.map { x => x.get("timeSpent").get.asInstanceOf[Double] });
            val meanInterruptTime = average(summaryEvents.map { x => x.get("interruptTime").get.asInstanceOf[Double] });

            val totalTimeSpentOnPlatform = sortedEvents.map { x => CommonUtil.getTimeDiff(x.context.date_range.from, x.context.date_range.to).get }.sum;

            val topKcontent = if (sortedGames.length > topK) sortedGames.take(topK).toArray else sortedGames.toArray;
            val meanActiveTimeOnPlatform = meanTimeSpent;
            val activeHours = summaryEvents.map { f =>
                try {
                    (CommonUtil.getHourOfDay(f.get("start_time").get.asInstanceOf[Long], f.get("end_time").get.asInstanceOf[Long]))
                } catch {
                    case ex: ClassCastException =>
                        null;
                }
            }.filter(_ != null).flatten.map { x => (x, 1) }.groupBy(_._1).map(x => (x._1, x._2.length));

            val mostActiveHrOfTheDay = if (activeHours.isEmpty) None else Option(activeHours.maxBy(f => f._2)._1);

            var meanTimeBtwnGamePlays = if (summaryEvents.length > 1) (CommonUtil.getTimeDiff(startTimestamp, endTimestamp).get - totalTimeSpentOnPlatform) / (summaryEvents.length - 1) else 0d
            if (meanTimeBtwnGamePlays < 0) meanTimeBtwnGamePlays = 0;

            TimeSummary(x.uid, DtRange(eventStartTimestamp, eventEndTimestamp), Option(meanTimeSpent), Option(CommonUtil.roundDouble(meanTimeBtwnGamePlays, 2)), Option(meanActiveTimeOnPlatform), Option(meanInterruptTime), Option(totalTimeSpentOnPlatform), meanTimeSpentOnAnAct, Option(meanCountOfAct), numOfSessionsOnPlatform, lastVisitTimeStamp, mostActiveHrOfTheDay, topKcontent, startTimestamp, endTimestamp);
        }
    }

    override def postProcess(data: RDD[TimeSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { summary =>
            val measures = Map(
                "meanTimeSpent" -> summary.meanTimeSpent,
                "meanTimeBtwnGamePlays" -> summary.meanTimeBtwnGamePlays,
                "meanActiveTimeOnPlatform" -> summary.meanActiveTimeOnPlatform,
                "meanInterruptTime" -> summary.meanInterruptTime,
                "totalTimeSpentOnPlatform" -> summary.totalTimeSpentOnPlatform,
                "meanTimeSpentOnAnAct" -> summary.meanTimeSpentOnAnAct,
                "meanCountOfAct" -> summary.meanCountOfAct,
                "numOfSessionsOnPlatform" -> summary.numOfSessionsOnPlatform,
                "last_visit_ts" -> summary.last_visit_ts,
                "mostActiveHrOfTheDay" -> summary.mostActiveHrOfTheDay,
                "topKcontent" -> summary.topKcontent,
                "start_ts" -> summary.start_ts,
                "end_ts" -> summary.end_ts);
            val mid = CommonUtil.getMessageId("ME_LEARNER_ACTIVITY_SUMMARY", summary.uid, "WEEK", summary.dtRange.to);
            MeasuredEvent("ME_LEARNER_ACTIVITY_SUMMARY", System.currentTimeMillis(), summary.dtRange.to, "1.0", mid, summary.uid, None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "LearnerActivitySummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "WEEK", summary.dtRange),
                Dimensions(None, null, None, None, None, None, None),
                MEEdata(measures));
        }
    }

    private def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
        CommonUtil.roundDouble(num.toDouble(ts.sum) / ts.size, 2)
    }
}