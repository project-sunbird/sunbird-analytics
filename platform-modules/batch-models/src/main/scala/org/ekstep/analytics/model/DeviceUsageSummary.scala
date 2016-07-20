package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.util.CommonUtil
import org.joda.time.LocalDate
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.JobLogger

case class UsageSummary(device_id: String, start_time: Long, end_time: Long, num_days: Long, total_launches: Long, total_timespent: Double,
                        avg_num_launches: Double, avg_time: Double, num_contents: Long, play_start_time: Long, last_played_on: Long,
                        total_play_time: Double, num_sessions: Long, mean_play_time: Double,
                        mean_play_time_interval: Double, previously_played_content: String) extends AlgoOutput

case class DeviceUsageInput(device_id: String, currentData: Buffer[DerivedEvent], previousData: Option[UsageSummary]) extends AlgoInput
case class DeviceId(device_id: String)

object DeviceUsageSummary extends IBatchModelTemplate[DerivedEvent, DeviceUsageInput, UsageSummary, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.DeviceUsageSummary"
    override def name: String = "DeviceUsageSummarizer"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceUsageInput] = {
        val filteredEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_GENIE_LAUNCH_SUMMARY")));
        val newGroupedEvents = filteredEvents.map(event => (event.dimensions.did.get, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b);
        val prevDeviceSummary = newGroupedEvents.map(f => DeviceId(f._1)).joinWithCassandraTable[UsageSummary](Constants.KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).map(f => (f._1.device_id, f._2))
        val deviceData = newGroupedEvents.leftOuterJoin(prevDeviceSummary);
        deviceData.map { x => DeviceUsageInput(x._1, x._2._1, x._2._2) }
    }

    override def algorithm(data: RDD[DeviceUsageInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[UsageSummary] = {

        val configMapping = sc.broadcast(config);
        data.map { events =>
            val eventsSortedByTS = events.currentData.sortBy { x => x.context.date_range.to };
            val eventsSortedByDateRange = events.currentData.sortBy { x => x.context.date_range.from };
            val prevUsageSummary = events.previousData.getOrElse(UsageSummary(events.device_id, 0L, 0L, 0L, 0L, 0.0, 0.0, 0.0, 0L, 0L, 0L, 0.0, 0L, 0.0, 0.0, ""));
            val tsInString = configMapping.value.getOrElse("startTime", "2015-03-01").asInstanceOf[String]
            val ts = CommonUtil.getTimestamp(tsInString, CommonUtil.dateFormat, "yyyy-MM-dd");
            val eventStartTime = if (eventsSortedByDateRange.head.context.date_range.from < ts) ts else eventsSortedByDateRange.head.context.date_range.from
            val start_time = if (prevUsageSummary.start_time == 0) eventStartTime else if (eventStartTime > prevUsageSummary.start_time) prevUsageSummary.start_time else eventStartTime;
            val end_time = eventsSortedByTS.last.context.date_range.to
            val num_days: Long = CommonUtil.daysBetween(new LocalDate(start_time), new LocalDate(end_time))
            val num_launches = events.currentData.size + prevUsageSummary.total_launches
            val avg_num_launches = if (num_days == 0) num_launches else CommonUtil.roundDouble((num_launches / (num_days.asInstanceOf[Double])), 2)
            val totalTimeSpent = events.currentData.map { x =>
                (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double])
            }.sum + prevUsageSummary.total_timespent
            val avg_time = if (num_days == 0) totalTimeSpent else CommonUtil.roundDouble(totalTimeSpent / num_days, 2)
            val num_contents = prevUsageSummary.num_contents
            val play_start_time = prevUsageSummary.play_start_time
            val last_played_on = prevUsageSummary.last_played_on
            val total_play_time = prevUsageSummary.total_play_time
            val num_sessions = prevUsageSummary.num_sessions
            val mean_play_time = prevUsageSummary.mean_play_time
            val mean_play_time_interval = prevUsageSummary.mean_play_time_interval
            val previously_played_content = prevUsageSummary.previously_played_content

            (UsageSummary(events.device_id, start_time, end_time, num_days, num_launches, totalTimeSpent, avg_num_launches, avg_time, num_contents, play_start_time, last_played_on, total_play_time, num_sessions, mean_play_time, mean_play_time_interval, previously_played_content))
        }.cache();
    }

    override def postProcess(data: RDD[UsageSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.saveToCassandra(Constants.KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE);
        data.map { usageSummary =>
            val mid = CommonUtil.getMessageId("ME_DEVICE_USAGE_SUMMARY", usageSummary.device_id, null, DtRange(0l, 0l));
            val measures = Map(
                "start_time" -> usageSummary.start_time,
                "end_time" -> usageSummary.end_time,
                "num_days" -> usageSummary.num_days,
                "avg_num_launches" -> usageSummary.avg_num_launches,
                "avg_time" -> usageSummary.avg_time,
                "num_contents" -> usageSummary.num_contents,
                "play_start_time" -> usageSummary.play_start_time,
                "last_played_on" -> usageSummary.last_played_on,
                "total_play_time" -> usageSummary.total_play_time,
                "num_sessions" -> usageSummary.num_sessions,
                "mean_play_time" -> usageSummary.mean_play_time,
                "mean_play_time_interval" -> usageSummary.mean_play_time_interval,
                "previously_played_content" -> usageSummary.previously_played_content);
            MeasuredEvent("ME_DEVICE_USAGE_SUMMARY", System.currentTimeMillis(), usageSummary.end_time, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "DeviceUsageSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "CUMULATIVE").asInstanceOf[String], DtRange(usageSummary.start_time, usageSummary.end_time)),
                Dimensions(None, Option(usageSummary.device_id), None, None, None, None, None),
                MEEdata(measures));
        }
    }
}