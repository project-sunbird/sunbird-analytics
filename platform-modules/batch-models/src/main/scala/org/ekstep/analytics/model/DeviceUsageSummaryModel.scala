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
import org.joda.time.DateTime

case class DeviceUsageSummary(device_id: String, start_time: Option[Long], end_time: Option[Long], num_days: Option[Long], total_launches: Option[Long], total_timespent: Option[Double],
                              avg_num_launches: Option[Double], avg_time: Option[Double], num_contents: Option[Long], play_start_time: Option[Long], last_played_on: Option[Long],
                              total_play_time: Option[Double], num_sessions: Option[Long], mean_play_time: Option[Double],
                              mean_play_time_interval: Option[Double], last_played_content: Option[String], updated_date: Option[DateTime] = Option(DateTime.now()))
case class DeviceUsageSummary_T(data: DeviceUsageSummary, syncts: Long) extends AlgoOutput

case class DeviceUsageInput(device_id: String, currentData: Buffer[DerivedEvent], previousData: Option[DeviceUsageSummary]) extends AlgoInput
case class DeviceId(device_id: String)

object DeviceUsageSummaryModel extends IBatchModelTemplate[DerivedEvent, DeviceUsageInput, DeviceUsageSummary_T, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.DeviceUsageSummaryModel"
    override def name: String = "DeviceUsageSummaryModel"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceUsageInput] = {
        val filteredEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_GENIE_LAUNCH_SUMMARY")));
        val newGroupedEvents = filteredEvents.map(event => (event.dimensions.did.get, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b);
        val prevDeviceSummary = newGroupedEvents.map(f => DeviceId(f._1)).joinWithCassandraTable[DeviceUsageSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE).map(f => (f._1.device_id, f._2))
        val deviceData = newGroupedEvents.leftOuterJoin(prevDeviceSummary);
        deviceData.map { x => DeviceUsageInput(x._1, x._2._1, x._2._2) }
    }

    override def algorithm(data: RDD[DeviceUsageInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceUsageSummary_T] = {

        val configMapping = sc.broadcast(config);
        data.map { events =>
            val eventsSortedByTS = events.currentData.sortBy { x => x.context.date_range.to };
            val endEvent = eventsSortedByTS.last
            val eventsSortedByDateRange = events.currentData.sortBy { x => x.context.date_range.from };
            val prevUsageSummary = events.previousData.getOrElse(DeviceUsageSummary(events.device_id, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None));
            val tsInString = configMapping.value.getOrElse("startTime", "2015-03-01").asInstanceOf[String]
            val ts = CommonUtil.getTimestamp(tsInString, CommonUtil.dateFormat, "yyyy-MM-dd");
            val eventStartTime = if (eventsSortedByDateRange.head.context.date_range.from < ts) ts else eventsSortedByDateRange.head.context.date_range.from
            val start_time = if (prevUsageSummary.start_time.isEmpty) eventStartTime else if (eventStartTime > prevUsageSummary.start_time.get) prevUsageSummary.start_time.get else eventStartTime;
            val end_time = endEvent.context.date_range.to
            val num_days: Long = CommonUtil.daysBetween(new LocalDate(start_time), new LocalDate(end_time))
            val num_launches = if (prevUsageSummary.total_launches.isEmpty) events.currentData.size else events.currentData.size + prevUsageSummary.total_launches.get
            val avg_num_launches = if (num_days == 0) num_launches else CommonUtil.roundDouble((num_launches / (num_days.asInstanceOf[Double])), 2)
            val current_ts = events.currentData.map { x =>
                (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double])
            }.sum
            val totalTimeSpent = if (prevUsageSummary.total_timespent.isEmpty) current_ts else current_ts + prevUsageSummary.total_timespent.get
            val avg_time = if (num_days == 0) totalTimeSpent else CommonUtil.roundDouble(totalTimeSpent / num_days, 2)

            DeviceUsageSummary_T(DeviceUsageSummary(events.device_id, Option(start_time), Option(end_time), Option(num_days), Option(num_launches), Option(totalTimeSpent), Option(avg_num_launches), Option(avg_time), prevUsageSummary.num_contents, prevUsageSummary.play_start_time, prevUsageSummary.last_played_on, prevUsageSummary.total_play_time, prevUsageSummary.num_sessions, prevUsageSummary.mean_play_time, prevUsageSummary.mean_play_time_interval, prevUsageSummary.last_played_content), endEvent.syncts)
        }.cache();
    }

    override def postProcess(data: RDD[DeviceUsageSummary_T], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { x => x.data }.saveToCassandra(Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE);
        data.map { x =>
            val usageSummary  = x.data
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
                "last_played_content" -> usageSummary.last_played_content);
            MeasuredEvent("ME_DEVICE_USAGE_SUMMARY", System.currentTimeMillis(), x.syncts, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "DeviceUsageSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "CUMULATIVE").asInstanceOf[String], DtRange(usageSummary.start_time.get, usageSummary.end_time.get)),
                Dimensions(None, Option(usageSummary.device_id), None, None, None, None, None),
                MEEdata(measures));
        }
    }
}